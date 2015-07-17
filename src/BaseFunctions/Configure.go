package BaseFunctions

import (
	"errors"
	"github.com/ewangplay/config"
	"strconv"
)

type Configure struct {
	ConfigureMap map[string]interface{}
}

func NewConfigure(filename string) (*Configure, error) {
	config := &Configure{}

	config.ConfigureMap = make(map[string]interface{})
	err := config.ParseConfigure(filename)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func (this *Configure) loopConfigure(sectionName string, cfg *config.Config) error {

	if cfg.HasSection(sectionName) {
		sec :=make(map[string]string)
		section, err := cfg.SectionOptions(sectionName)
		if err == nil {
			for _, v := range section {
				options, err := cfg.String(sectionName, v)
				if err == nil {
					
					sec[v] = options
				}
			}
			this.ConfigureMap[sectionName]=sec
			return nil
		}
		return errors.New("Parse Error")
	}

	return errors.New("No Section")
}

func (this *Configure) ParseConfigure(filename string) error {
	cfg, err := config.ReadDefault(filename)
	if err != nil {
		return err
	}


	this.loopConfigure("mysql", cfg)
	this.loopConfigure("sql", cfg)
	this.loopConfigure("table", cfg)
	return nil
}



func (this *Configure) GetTableFields() (map[string]string,error) {
	
	fields,ok := this.ConfigureMap["table"].(map[string]string)
	if ok == false {
		return nil, errors.New("No SqlSentence,use defualt")
	}
	
	return fields,nil
}


func (this *Configure) GetSqlSentence() (string, error) {
	v,ok := this.ConfigureMap["sql"].(map[string]string)
	if ok == false {
		return "", errors.New("No SqlSentence,use defualt")
	}
	
	SqlSentence, ok :=v["sqlsentence"]

	if ok == false {
		return "", errors.New("No SqlSentence,use defualt")
	}

	return SqlSentence, nil
}


//数据库连接配置信息
func (this *Configure) GetMysqlUserName() (string, error) {

	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return "", errors.New("No,use defualt")
	}
	mysqlusername, ok :=v["mysqlusername"]

	if ok == false {
		return "root", errors.New("No mysqlusername,use defualt")
	}

	return mysqlusername, nil
}

func (this *Configure) GetMysqlPassword() (string, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return "", errors.New("No,use defualt")
	}
	mysqlpassword, ok := v["mysqlpassword"]

	if ok == false {
		return "12345", errors.New("No mysqlpassword,use defualt")
	}

	return mysqlpassword, nil
}

func (this *Configure) GetMysqlHost() (string, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return "", errors.New("No,use defualt")
	}
	mysqlhost, ok := v["mysqlhost"]

	if ok == false {
		return "127.0.0.1", errors.New("No mysqlhost,use defualt")
	}

	return mysqlhost, nil
}

func (this *Configure) GetMysqlPort() (string, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return "", errors.New("No,use defualt")
	}
	mysqlport, ok := v["mysqlport"]

	if ok == false {
		return "3306", errors.New("No mysqlport,use defualt")
	}

	return mysqlport, nil
}

func (this *Configure) GetMysqlDBname() (string, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return "", errors.New("No,use defualt")
	}
	mysqlDBname, ok := v["mysqlDBname"]

	if ok == false {
		return "test", errors.New("No mysqlDBname,use defualt")
	}

	return mysqlDBname, nil
}

func (this *Configure) GetMysqlCharset() (string, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return "", errors.New("No,use defualt")
	}
	mysqlcharset, ok := v["mysqlcharset"]

	if ok == false {
		return "utf8", errors.New("No mysqlcharset,use defualt")
	}

	return mysqlcharset, nil
}

func (this *Configure) GetMysqlMaxConns() (int, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return 0, errors.New("No,use defualt")
	}
	mysqlmaxconnsstr, ok := v["mysqlmaxconns"]
	if ok == false {
		return 9090, errors.New("No mysqlmaxconns set, use default")
	}

	mysqlmaxconns, err := strconv.Atoi(mysqlmaxconnsstr)
	if err != nil {
		return 2000, err
	}

	return mysqlmaxconns, nil
}

func (this *Configure) GetMysqlMaxIdleConns() (int, error) {
	v,ok := this.ConfigureMap["mysql"].(map[string]string)
	if ok == false {
		return 0, errors.New("No,use defualt")
	}
	mysqlmaxidleconnsstr, ok := v["mysqlmaxidleconns"]
	if ok == false {
		return 9090, errors.New("No mysqlmaxidleconns set, use default")
	}

	mysqlmaxidleconns, err := strconv.Atoi(mysqlmaxidleconnsstr)
	if err != nil {
		return 1000, err
	}

	return mysqlmaxidleconns, nil
}
