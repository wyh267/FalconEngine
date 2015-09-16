package plugins


type QueryRules struct{
	
}


func StringInList(target string,sources []string) bool {
	
	for _,source := range sources{
		if target == source {
			return true
		}
	}
	return false
}