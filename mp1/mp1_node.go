/* types: bool
 * 		0: deposit 1: transfer
 */
func handle_balance(types bool, acct1 string, acct2 string, money int) {
	// transfer account1 -> account2
	if types {
		person1 := findAcctByName(acct1)
		if person1 == nil {
			return
		}
		if (person1.acct_bala - money) < 0 {
			return
		} else {
			person1.acct_bala = person1.acct_bala - money
		}
		person2 := findAcctByName(acct2)
		if person2 == nil {
			person := account{acct1, money, true}
			acct_list = append(acct_list, person)
			sort.Slice(acct_list, func(i, j int) bool {
				return acct_list[i].acct_name < acct_list[j].acct_name
			})
		} else {
			person2.acct_bala = person2.acct_bala + money
		}
	} else {
		// deposit  account1 =  account2 from=to
		person1 := findAcctByName(acct1)
		if person1 == nil {
			person := account{acct1, money, true}
			acct_list = append(acct_list, person)
			//Sort the slice by name field in alphabetical order
			sort.Slice(acct_list, func(i, j int) bool {
				return acct_list[i].acct_name < acct_list[j].acct_name
			})
		} else {
			person1.acct_bala = money
		}
	}
	// print the balance of each accts
	fmt.Println("Balance ")
	for i := 0; i < len(acct_list); i++ {
		fmt.Println(acct_list[i].acct_name + ":" + strconv.Itoa(acct_list[i].acct_bala) + " ")
	}
}



