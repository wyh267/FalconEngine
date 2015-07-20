

package utils

import (
    "sort"
)

func RemoveDuplicatesAndEmpty(a []string) (ret []string){
    sort.Strings(a)
    a_len := len(a)
    for i:=0; i < a_len; i++{
        if (i > 0 && a[i-1] == a[i]) || len(a[i])==0{
            continue;
        }
        ret = append(ret, a[i])
    }
    return
}



func Interaction(a []DocIdInfo , b []DocIdInfo) ([]DocIdInfo,bool){
    
    lena := len(a)
    lenb := len(b)
    c := make([]DocIdInfo,0)

    ia:=0
    ib:=0
    for ia<lena && ib<lenb {
        
        if a[ia].DocId == b[ib].DocId{
            c=append(c,a[ia])
        }
        
        if a[ia].DocId < b[ib].DocId{
            ia ++
        }else{
            ib ++
        }
    }
    
    if len(c) == 0 {
        return nil,false
    }else{
        return c,true
    }
    
}