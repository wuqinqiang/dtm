package examples

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/yedf/dtm/common"
	"github.com/yedf/dtm/dtmcli"
	"gorm.io/gorm"
)

var XaClient *dtmcli.XaClient = nil

type UserAccount struct {
	common.ModelBase
	UserId  int
	Balance string
}

func (u *UserAccount) TableName() string { return "user_account" }

type UserAccountTrading struct {
	common.ModelBase
	UserId         int
	TradingBalance string
}

func (u *UserAccountTrading) TableName() string { return "user_account_trading" }

func dbGet() *common.DB {
	return common.DbGet(Config.Mysql)
}

func XaFireRequest() {
	_, err := XaClient.XaGlobalTransaction(func(xa *dtmcli.Xa) (rerr error) {
		defer common.P2E(&rerr)
		req := GenTransReq(30, false, false)
		resp, err := xa.CallBranch(req, Busi+"/TransOutXa")
		common.CheckRestySuccess(resp, err)
		resp, err = xa.CallBranch(req, Busi+"/TransInXa")
		common.CheckRestySuccess(resp, err)
		return nil
	})
	e2p(err)
}

// api
func XaSetup(app *gin.Engine) {
	app.POST(BusiApi+"/TransInXa", common.WrapHandler(xaTransIn))
	app.POST(BusiApi+"/TransOutXa", common.WrapHandler(xaTransOut))
	Config.Mysql["database"] = "dtm_busi"
	XaClient = dtmcli.NewXaClient(DtmServer, Config.Mysql, app, Busi+"/xa")
}

func xaTransIn(c *gin.Context) (interface{}, error) {
	err := XaClient.XaLocalTransaction(c, func(db *common.DB, xa *dtmcli.Xa) (rerr error) {
		req := reqFrom(c)
		if req.TransInResult != "SUCCESS" {
			return fmt.Errorf("tranIn failed")
		}
		dbr := db.Model(&UserAccount{}).Where("user_id = ?", 2).
			Update("balance", gorm.Expr("balance + ?", req.Amount))
		return dbr.Error
	})
	e2p(err)
	return M{"result": "SUCCESS"}, nil
}

func xaTransOut(c *gin.Context) (interface{}, error) {
	err := XaClient.XaLocalTransaction(c, func(db *common.DB, xa *dtmcli.Xa) (rerr error) {
		req := reqFrom(c)
		if req.TransOutResult != "SUCCESS" {
			return fmt.Errorf("tranOut failed")
		}
		dbr := db.Model(&UserAccount{}).Where("user_id = ?", 1).
			Update("balance", gorm.Expr("balance - ?", req.Amount))
		return dbr.Error
	})
	e2p(err)
	return M{"result": "SUCCESS"}, nil
}

func ResetXaData() {
	db := dbGet()
	db.Must().Exec("truncate user_account")
	db.Must().Exec("insert into user_account (user_id, balance) values (1, 10000), (2, 10000)")
	type XaRow struct {
		Data string
	}
	xas := []XaRow{}
	db.Must().Raw("xa recover").Scan(&xas)
	for _, xa := range xas {
		db.Must().Exec(fmt.Sprintf("xa rollback '%s'", xa.Data))
	}
}
