

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