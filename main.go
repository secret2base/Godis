package main

import "fmt"

func main() {
	// 定义三个数
	a := 5
	b := 3
	c := 2

	// 计算 (a xor b) * c
	result1 := (a ^ b) * c

	// 计算 (a * c) xor b
	result2 := (a * c) ^ b

	// 比较两个结果是否相同
	areEqual := result1 == result2

	// 输出结果
	fmt.Printf("(a xor b) * c = %d\n", result1)
	fmt.Printf("(a * c) xor b = %d\n", result2)
	fmt.Printf("Are they equal? %t\n", areEqual)
}
