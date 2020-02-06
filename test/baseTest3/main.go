/*
@Time       : 2020/1/10 11:55 上午
@Author     : lei
@File       : main
@Software   : GoLand
@Desc       :
*/
package main

import (
	"fmt"
	"strings"
)

/*
滑动窗口
给定一个字符串，请你找出其中不含有重复字符的 最长子串 的长度。

示例 1:

输入: "abcabcbb"
输出: 3
解释: 因为无重复字符的最长子串是 "abc"，所以其长度为 3。
示例 2:

输入: "bbbbb"
输出: 1
解释: 因为无重复字符的最长子串是 "b"，所以其长度为 1。
示例 3:

输入: "pwwkew"
输出: 3
解释: 因为无重复字符的最长子串是 "wke"，所以其长度为 3。
     请注意，你的答案必须是 子串 的长度，"pwke" 是一个子序列，不是子串。

来源：力扣（LeetCode）
链接：https://leetcode-cn.com/problems/longest-substring-without-repeating-characters
著作权归领扣网络所有。商业转载请联系官方授权，非商业转载请注明出处。
*/
func lengthOfLongestSubstring(s string) int {
	maxLen, left, curLen := 0, 0, 0

	for i := 0; i < len(s); i++ {
		for {
			isExist := strings.IndexByte(s[left:i], s[i])
			if isExist == -1 {
				curLen++
				break
			} else {
				left++
				curLen = len(s[left:i]) // 剔除元素后，重新计算长度
			}

		}
		if curLen > maxLen {
			maxLen = curLen
		}

	}
	return maxLen
}

func main() {

	s := "abcabcbb"
	fmt.Println(lengthOfLongestSubstring(s))

}
