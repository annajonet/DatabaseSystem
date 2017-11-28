package dubstep;

import java.sql.SQLException;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;

class Projection{
	
	QueryProcessor qp;
	
	public Projection(QueryProcessor qp){
		this.qp = qp;
	}
	
	public PrimitiveValue[] Project(PrimitiveValue[] cols) throws SQLException{
		PrimitiveValue[] col_val = new PrimitiveValue[qp.n_queryargs];
		PrimitiveValue[] old_columns = qp.cols;
		qp.cols = cols;
		for(int i=0; i<qp.n_queryargs; i++){
			col_val[i] = qp.e.eval(qp.prj_expr.get(i));
		}
		qp.cols = old_columns;
		return col_val;
	}
	
	public boolean hasProjNext() throws InvalidPrimitive, SQLException{
		return qp.hasReadRecord();
	}
	
}
