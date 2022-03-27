from libmysql_utils.mysql8 import mysqlMeta, mysqlHeader
from libmysql_utils.orm.form import formStock
import datetime
from sqlalchemy import update, select


class FormORMStock(mysqlMeta):
    database = 'stock'
    table_name = 'stock_template'
    def __init__(self, acc: str, pw: str, host='115.159.1.221'):
        header = mysqlHeader(acc, pw, self.database, host)
        super().__init__(header)

    def query(self, stock_code: str):
        formStock.__table__.name = 'SH600000'
        sql = select(formStock.trade_date,formStock.stock_name,formStock.close_price)
        result = self.session.execute(sql).all()
        return result

    def update_factor(self, stock_code: str, trade_date: str, adjust_factor: float):
        formStock.__table__.name = stock_code
        sql = update(formStock).filter(formStock.trade_date==trade_date).values(adjust_factor=adjust_factor).execution_options(synchronize_session="fetch")
        self.session.execute(sql)
        self.session.commit()
