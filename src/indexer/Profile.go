/*****************************************************************************
 *  file name : Profile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 正排索引
 *
******************************************************************************/

package indexer

const (
	PflNum	= iota
	PflText
	PflDate
)

type Profile struct{
	Name  		string
	Type		int64
	Len			int64
}


/*****************************************************************************
*  function name : GetMaxDocId
*  params : nil
*  return : int64	
*
*  description : get profile's length, max doc_id number
*
******************************************************************************/

func (this *Profile)GetMaxDocId() int64{
	return this.Len-1
}

/*****************************************************************************
*  function name : GetProfileName
*  params : nil	
*  return : string
*
*  description : get profile name 
*
******************************************************************************/

func (this *Profile)GetName() string {
	return this.Name
}

