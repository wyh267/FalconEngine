package mlog


import (
"fmt"
"io"
"io/ioutil"
"log"
"os"
"path"
"sync/atomic"
)

// LogLevel type
type LogLevel int32

const (
	// LevelTrace logs everything
	LevelTrace LogLevel = (1 << iota)

	// LevelInfo logs Info, Warnings and Errors
	LevelInfo

	// LevelWarn logs Warning and Errors
	LevelWarn

	// LevelError logs just Errors
	LevelError
)

const MaxBytes int = 100 * 1024 * 1024
const BackupCount int = 10

type mlog struct {
	LogLevel int32

	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
	Fatal   *log.Logger

	LogFile *RotatingFileHandler
}

const (
	color_red = uint8(iota + 91)
	color_green
	color_yellow
	color_blue
	color_magenta //洋红
)

var Logger mlog

// DefaultFlags used by created loggers
var DefaultFlags = log.Ldate | log.Ltime | log.Lshortfile

//RotatingFileHandler writes log a file, if file size exceeds maxBytes,
//it will backup current file and open a new one.
//
//max backup file number is set by backupCount, it will delete oldest if backups too many.
type RotatingFileHandler struct {
	fd *os.File

	fileName    string
	maxBytes    int
	backupCount int
}

// NewRotatingFileHandler creates dirs and opens the logfile
func NewRotatingFileHandler(fileName string, maxBytes int, backupCount int) (*RotatingFileHandler, error) {
	dir := path.Dir(fileName)
	os.Mkdir(dir, 0777)

	h := new(RotatingFileHandler)

	if maxBytes <= 0 {
		return nil, fmt.Errorf("invalid max bytes")
	}

	h.fileName = fileName
	h.maxBytes = maxBytes
	h.backupCount = backupCount

	var err error
	h.fd, err = os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (h *RotatingFileHandler) Write(p []byte) (n int, err error) {
	h.doRollover()
	return h.fd.Write(p)
}

// Close simply closes the File
func (h *RotatingFileHandler) Close() error {
	if h.fd != nil {
		return h.fd.Close()
	}
	return nil
}

func (h *RotatingFileHandler) doRollover() {
	f, err := h.fd.Stat()
	if err != nil {
		return
	}

	// log.Println("size: ", f.Size())

	if h.maxBytes <= 0 {
		return
	} else if f.Size() < int64(h.maxBytes) {
		return
	}

	if h.backupCount > 0 {
		h.fd.Close()

		for i := h.backupCount - 1; i > 0; i-- {
			sfn := fmt.Sprintf("%s.%d", h.fileName, i)
			dfn := fmt.Sprintf("%s.%d", h.fileName, i+1)

			os.Rename(sfn, dfn)
		}

		dfn := fmt.Sprintf("%s.1", h.fileName)
		os.Rename(h.fileName, dfn)

		h.fd, _ = os.OpenFile(h.fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	}
}

// Start starts the logging
func Start(level LogLevel, path string) {
	doLogging(level, path, MaxBytes, BackupCount)
}

func StartEx(level LogLevel, path string, maxBytes, backupCount int) {
	doLogging(level, path, maxBytes, backupCount)
}

// Stop stops the logging
func Stop() error {
	if Logger.LogFile != nil {
		return Logger.LogFile.Close()
	}

	return nil
}

//Sync commits the current contents of the file to stable storage.
//Typically, this means flushing the file system's in-memory copy
//of recently written data to disk.
func Sync() {
	if Logger.LogFile != nil {
		Logger.LogFile.fd.Sync()
	}
}

func doLogging(logLevel LogLevel, fileName string, maxBytes, backupCount int) {
	traceHandle := ioutil.Discard
	infoHandle := ioutil.Discard
	warnHandle := ioutil.Discard
	errorHandle := ioutil.Discard
	fatalHandle := ioutil.Discard

	var fileHandle *RotatingFileHandler

	switch logLevel {
	case LevelTrace:
		traceHandle = os.Stdout
		fallthrough
	case LevelInfo:
		infoHandle = os.Stdout
		fallthrough
	case LevelWarn:
		warnHandle = os.Stdout
		fallthrough
	case LevelError:
		errorHandle = os.Stderr
		fatalHandle = os.Stderr
	}

	if fileName != "" {
		var err error
		fileHandle, err = NewRotatingFileHandler(fileName, maxBytes, backupCount)
		if err != nil {
			log.Fatal("mlog: unable to create RotatingFileHandler: ", err)
		}

		if traceHandle == os.Stdout {
			traceHandle = io.MultiWriter(fileHandle, traceHandle)
		}

		if infoHandle == os.Stdout {
			infoHandle = io.MultiWriter(fileHandle, infoHandle)
		}

		if warnHandle == os.Stdout {
			warnHandle = io.MultiWriter(fileHandle, warnHandle)
		}

		if errorHandle == os.Stderr {
			errorHandle = io.MultiWriter(fileHandle, errorHandle)
		}

		if fatalHandle == os.Stderr {
			fatalHandle = io.MultiWriter(fileHandle, fatalHandle)
		}
	}

	Logger = mlog{
		Trace:   log.New(traceHandle, yellow("[TRACE]: "), DefaultFlags),
		Info:    log.New(infoHandle, green("[INFO ]: "), DefaultFlags),
		Warning: log.New(warnHandle, magenta("[WARN ]: "), DefaultFlags),
		Error:   log.New(errorHandle, red("[ERROR]: "), DefaultFlags),
		Fatal:   log.New(errorHandle, blue("[FATAL]: "), DefaultFlags),
		LogFile: fileHandle,
	}

	atomic.StoreInt32(&Logger.LogLevel, int32(logLevel))
}

//** TRACE

// Trace writes to the Trace destination
func Trace(format string, a ...interface{}) {
	Logger.Trace.Output(2, fmt.Sprintf(format, a...))
}

//** INFO

// Info writes to the Info destination
func Info(format string, a ...interface{}) {
	Logger.Info.Output(2, fmt.Sprintf(format, a...))
}

//** WARNING

// Warning writes to the Warning destination
func Warning(format string, a ...interface{}) {
	Logger.Warning.Output(2, magenta(fmt.Sprintf(format, a...)))
}

//** ERROR

// Error writes to the Error destination and accepts an err
func Error(format string, a ...interface{} /*err error*/) {
	Logger.Error.Output(2, red(fmt.Sprintf(format, a...)))
	//Logger.Error.Output(2, fmt.Sprintf("%s\n", err))
}

// IfError is a shortcut function for log.Error if error
func IfError(err error) {
	if err != nil {
		Logger.Error.Output(2, fmt.Sprintf("%s\n", err))
	}
}

//** FATAL

// Fatal writes to the Fatal destination and exits with an error 255 code
func Fatal(a ...interface{}) {
	Logger.Fatal.Output(2, fmt.Sprint(a...))
	Sync()
	os.Exit(255)
}

// Fatalf writes to the Fatal destination and exits with an error 255 code
func Fatalf(format string, a ...interface{}) {
	Logger.Fatal.Output(2, fmt.Sprintf(format, a...))
	Sync()
	os.Exit(255)
}

// FatalIfError is a shortcut function for log.Fatalf if error and
// exits with an error 255 code
func FatalIfError(err error) {
	if err != nil {
		Logger.Fatal.Output(2, fmt.Sprintf("%s\n", err))
		Sync()
		os.Exit(255)
	}
}

func red(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_red, s)
}
func green(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_green, s)
}
func yellow(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_yellow, s)
}
func blue(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_blue, s)
}
func magenta(s string) string {
	return fmt.Sprintf("\x1b[%dm%s\x1b[0m", color_magenta, s)
}

func Red(s string) string {
	return red(s)
}
func Green(s string) string {
	return green(s)
}
func Yellow(s string) string {
	return yellow(s)
}
func Blue(s string) string {
	return blue(s)
}
func Magenta(s string) string {
	return magenta(s)
}

