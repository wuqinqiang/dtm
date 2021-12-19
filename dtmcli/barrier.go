/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmcli

import (
	"database/sql"
	"fmt"
	"github.com/yedf/dtm/dtmcli/dtmimp"
	"net/url"
)

// BarrierBusiFunc type for busi func
type BarrierBusiFunc func(tx *sql.Tx) error

// BranchBarrier every branch info
type BranchBarrier struct {
	TransType string
	Gid       string
	BranchID  string
	Op        string
	BarrierID int
}

func (bb *BranchBarrier) String() string {
	return fmt.Sprintf("transInfo: %s %s %s %s", bb.TransType, bb.Gid, bb.BranchID, bb.Op)
}

// BarrierFromQuery construct transaction info from request
func BarrierFromQuery(qs url.Values) (*BranchBarrier, error) {
	return BarrierFrom(qs.Get("trans_type"), qs.Get("gid"), qs.Get("branch_id"), qs.Get("op"))
}

// BarrierFrom construct transaction info from request
func BarrierFrom(transType, gid, branchID, op string) (*BranchBarrier, error) {
	ti := &BranchBarrier{
		TransType: transType,
		Gid:       gid,
		BranchID:  branchID,
		Op:        op,
	}
	if ti.TransType == "" || ti.Gid == "" || ti.BranchID == "" || ti.Op == "" {
		return nil, fmt.Errorf("invlid trans info: %v", ti)
	}
	return ti, nil
}

func insertBarrier(tx DB, transType string, gid string, branchID string, op string, barrierID string, reason string) (int64, error) {
	if op == "" {
		return 0, nil
	}
	sql := dtmimp.GetDBSpecial().GetInsertIgnoreTemplate("dtm_barrier.barrier(trans_type, gid, branch_id, op, barrier_id, reason) values(?,?,?,?,?,?)", "uniq_barrier")
	return dtmimp.DBExec(tx, sql, transType, gid, branchID, op, barrierID, reason)
}

//func findBarrier(tx DB, transType string, gid string, branchID string, op string) (int64, error) {
//	if op == "" {
//		return 0, nil
//	}
//	sql := dtmimp.GetDBSpecial().GetInsertIgnoreTemplate("dtm_barrier.barrier(trans_type, gid, branch_id, op, barrier_id, reason) values(?,?,?,?,?,?)", "uniq_barrier")
//	return dtmimp.DBExec(tx, sql, transType, gid, branchID, op, barrierID, reason)
//}

// Call Sub-transaction barrier,see for details: https://zhuanlan.zhihu.com/p/388444465
// tx: Transaction objects of the local database, allowing sub-transaction barriers to perform transaction operations
// busiCall: business func,called only when necessary
func (bb *BranchBarrier) Call(tx *sql.Tx, busiCall BarrierBusiFunc) (rerr error) {
	bb.BarrierID = bb.BarrierID + 1
	defer func() {
		if x := recover(); x != nil {
			tx.Rollback()
			panic(x)
		} else if rerr != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()
	// check this request
	if !bb.checkRepeatOp(tx) || !bb.checkEmptyRollBackOp(tx) {
		return
	}

	rerr = busiCall(tx)
	return
}

// Guaranteed same request idempotence
func (bb *BranchBarrier) checkRepeatOp(tx *sql.Tx) bool {
	bid := fmt.Sprintf("%02d", bb.BarrierID)
	currentAffected, _ := insertBarrier(tx, bb.TransType,
		bb.Gid, bb.BranchID, bb.Op, bid, bb.Op)
	if currentAffected == 0 {
		return false
	}
	return true
}

// Without calling TCC | saga try method,the second-stage Cancel method was
// called.the Cancel method needs to recognize that this is an empty rollback,
// and then directly return success
func (bb *BranchBarrier) checkEmptyRollBackOp(tx *sql.Tx) bool {
	if bb.Op != BranchCancel && bb.Op != BranchCompensate {
		return true
	}
	bid := fmt.Sprintf("%02d", bb.BarrierID)

	var (
		originType string
	)
	if bb.Op == BranchCancel {
		originType = BranchTry
	} else if bb.Op == BranchCompensate {
		originType = BranchAction
	}

	// we insert a piece of gid-branchid-try data. when originAffected > 0,
	// it means that the first stage try action has not been executed yet.
	// so we can know this request is a empty rollback op.
	// and this also means that the second stage (cancel | compensate) action has been executed before try,
	// if the try method finally comes, the unique key cannot be inserted repeatedly,
	// this indirectly solves the suspension op
	if originAffected, _ := insertBarrier(tx, bb.TransType,
		bb.Gid, bb.BranchID, originType, bid, bb.Op); originAffected > 0 {
		return false
	}
	return true
}

// CallWithDB the same as Call, but with *sql.DB
func (bb *BranchBarrier) CallWithDB(db *sql.DB, busiCall BarrierBusiFunc) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	return bb.Call(tx, busiCall)
}
