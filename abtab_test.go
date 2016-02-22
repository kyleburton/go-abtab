// see: http://www.golang-book.com/books/intro/12
package abtab

import "testing"

func TestAverage(t *testing.T) {
	if true != true {
		t.Error("Expected 1.5, got ")
	}
}

/*
rewrite the shell based tests to call runProgram() with sensible argument arrays instead of using the external shell scripts

*/

// see: https://github.com/codegangsta/cli
