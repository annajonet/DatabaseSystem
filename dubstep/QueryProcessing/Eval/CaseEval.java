package dubstep;

import java.sql.SQLException;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;

class CaseEval{
	
	QueryProcessor qrtpr;
	Eval e;
	
	public CaseEval(QueryProcessor qp){
		
		e = new Eval() {
			
			@Override
			public PrimitiveValue eval(Column arg0) throws SQLException {
				// TODO Auto-generated method stub
				int index = 0;
				try{
					if(qrtpr.join_list==null)
						index = qrtpr.Qp_mapper.get(arg0.getColumnName());
					else{
						if(arg0.getTable().getName()==null)
							index = qrtpr.Qp_mapper.get(arg0.getColumnName());
						else
							index = qrtpr.Qp_mapper.get(arg0.getTable().getName()+"_"+arg0.getColumnName());
					}
				}
				catch(Exception e){
				}

				return qrtpr.cols[index];

			}
		};

	}
	
	
	
}