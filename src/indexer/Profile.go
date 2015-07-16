/*****************************************************************************
 *  file name : Profile.go
 *  author : Wu Yinghao
 *  email  : wyh817@gmail.com
 *
 *  file description : 正排索引
 *
******************************************************************************/

package indexer

type Profile struct{
	Name  		string
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

func GetMaxDocId() int64{
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

func GetName() string {
	return this.Name
}

