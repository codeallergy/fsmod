/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package fsmod

import (
	"github.com/codeallergy/glue"
	"github.com/sprintframework/fs"
)

type fsScanner struct {
	Scan     []interface{}
}

func Scanner(scan... interface{}) glue.Scanner {
	return &fsScanner{
		Scan: scan,
	}
}

func (t *fsScanner) Beans() []interface{} {

	beans := []interface{}{
		FileService(),
		&struct {
			FileService []fs.FileService `inject`
		}{},
	}

	return append(beans, t.Scan...)
}

