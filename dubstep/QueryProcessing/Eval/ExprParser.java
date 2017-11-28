package dubstep;

import java.sql.SQLException;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

class exprParser{
	String tab_list;
	int exp_type = -1;
	Expression exp;
	
	public exprParser(Expression e) {
		// TODO Auto-generated constructor stub
		exp = e;
		
	}
}

class ExprEval{
	
	Eval e;
	TableIterator tab_it;
	IndexIterator idx_it;
	boolean indexed;
	
	public ExprEval(TableIterator it){
		tab_it = it;
		indexed = false;
		e = new Eval() {
			
			@Override
			public PrimitiveValue eval(Column arg0) throws SQLException {
				// TODO Auto-generated method stub
				
				return tab_it.getColVal(arg0);
			}
		};
	}
	public ExprEval(IndexIterator it){
		idx_it = it;
		indexed = true;
		e = new Eval() {
			
			@Override
			public PrimitiveValue eval(Column arg0) throws SQLException {
				// TODO Auto-generated method stub
				return idx_it.getColVal(arg0);   //////////////////////// why it was return null earlier??
				
			}
		};
	}
	
	public PrimitiveValue eval(Expression expr) throws SQLException{
		
		return e.eval(expr);
	}
	
}