package dubstep;

import java.io.IOException;
import java.sql.SQLException;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

class Evaluator{
	IndexIterator idx_it;
	TabexperIterator tab_expr_it;
	TableIterator tab_it;
	int indexed;
	int exp_type;
//	QTable table

	Evaluator(QTable table, Expression where_exp) throws InvalidPrimitive, SQLException, IOException{
		if(where_exp instanceof EqualsTo){
//			System.out.println("Entered Equal expression!");
			EqualsTo a = (EqualsTo) where_exp;
			exp_type = 0;
			Column lhs; Expression re;
			if(a.getLeftExpression() instanceof Column){
				lhs = (Column) a.getLeftExpression();
				re = a.getRightExpression();
			}
			else{
				lhs = (Column) a.getRightExpression();
				re = a.getLeftExpression();
			}
			PrimitiveValue rhs = null;
			if(re instanceof Function){
				Function fn = (Function)re;
				String fn_nm = ((Function) re).getName().toUpperCase();
				if(fn_nm.equals("DATE")){
					rhs = new DateValue(fn.getParameters().getExpressions().get(0).toString().replaceAll("'", ""));
				}
			}
			if(re instanceof DateValue){
				rhs = (DateValue)re;
			}
			else if(re instanceof LongValue){
				rhs = (LongValue)re;
			}
			else if(re instanceof DoubleValue){
				rhs = (DoubleValue)re;
			}
			else if(re instanceof StringValue){
				rhs = (StringValue)re;
			}
			else if(re instanceof BooleanValue){
				rhs = (BooleanValue)re;
			}
			
		    if(lhs == null || rhs == null) indexed = -1;
		    else{
		    	Table t = lhs.getTable();
		    	String col_nm = lhs.getColumnName();
		    	QIndex idx = table.s_index;
		    	if(table.hasIndex(col_nm)){
		    		idx_it = new IndexIterator(idx, col_nm, rhs, exp_type);
		    		indexed = 1;
		    	}
		    	else{
		    		tab_expr_it = new TabexperIterator(table, where_exp);
		    		indexed = 0;
		    	}
		    }
		    
		}
		else if(where_exp instanceof NotEqualsTo){
			NotEqualsTo a = (NotEqualsTo) where_exp;
			exp_type = 1;
			Column lhs; Expression re;
			if(a.getLeftExpression() instanceof Column){
				lhs = (Column) a.getLeftExpression();
				re = a.getRightExpression();
			}
			else{
				lhs = (Column) a.getRightExpression();
				re = a.getLeftExpression();
			}
			PrimitiveValue rhs = null;
			if(re instanceof Function){
				Function fn = (Function)re;
				String fn_nm = ((Function) re).getName().toUpperCase();
				if(fn_nm.equals("DATE")){
					rhs = new DateValue(fn.getParameters().getExpressions().get(0).toString().replaceAll("'", ""));
				}
			}
			if(re instanceof DateValue){
				rhs = (DateValue)re;
			}
			else if(re instanceof LongValue){
				rhs = (LongValue)re;
			}
			else if(re instanceof DoubleValue){
				rhs = (DoubleValue)re;
			}
			else if(re instanceof StringValue){
				rhs = (StringValue)re;
			}
			else if(re instanceof BooleanValue){
				rhs = (BooleanValue)re;
			}
			
		    if(lhs == null || rhs == null) indexed = -1;
		    else{
		    	Table t = lhs.getTable();
		    	String col_nm = lhs.getColumnName();
		    	QIndex idx = table.s_index;
		    	if(table.hasIndex(col_nm)){
		    		idx_it = new IndexIterator(idx, col_nm, rhs, exp_type);
		    		indexed = 1;
		    	}
		    	else{
		    		tab_expr_it = new TabexperIterator(table, where_exp);
		    		indexed = 0;
		    	}
		    }
		}
		else if(where_exp instanceof GreaterThan){
			GreaterThan a = (GreaterThan) where_exp;
			exp_type = 2;
			Column lhs; Expression re;
			if(a.getLeftExpression() instanceof Column){
				lhs = (Column) a.getLeftExpression();
				re = a.getRightExpression();
			}
			else{
				lhs = (Column) a.getRightExpression();
				re = a.getLeftExpression();
			}
			PrimitiveValue rhs = null;
			if(re instanceof Function){
				Function fn = (Function)re;
				String fn_nm = ((Function) re).getName().toUpperCase();
				if(fn_nm.equals("DATE")){
					rhs = new DateValue(fn.getParameters().getExpressions().get(0).toString().replaceAll("'", ""));
				}
			}
			if(re instanceof DateValue){
				rhs = (DateValue)re;
			}
			else if(re instanceof LongValue){
				rhs = (LongValue)re;
			}
			else if(re instanceof DoubleValue){
				rhs = (DoubleValue)re;
			}
			else if(re instanceof StringValue){
				rhs = (StringValue)re;
			}
			else if(re instanceof BooleanValue){
				rhs = (BooleanValue)re;
			}
			
		    if(lhs == null || rhs == null) indexed = -1;
		    else{
		    	Table t = lhs.getTable();
		    	String col_nm = lhs.getColumnName();
		    	QIndex idx = table.s_index;
		    	if(table.hasIndex(col_nm)){
		    		idx_it = new IndexIterator(idx, col_nm, rhs, exp_type);
		    		indexed = 1;
		    	}
		    	else{
		    		tab_expr_it = new TabexperIterator(table, where_exp);
		    		indexed = 0;
		    	}
		    }
		}

		else if(where_exp instanceof GreaterThanEquals){
			GreaterThanEquals a = (GreaterThanEquals) where_exp;
			exp_type = 3;
			Column lhs; Expression re;
			if(a.getLeftExpression() instanceof Column){
				lhs = (Column) a.getLeftExpression();
				re = a.getRightExpression();
			}
			else{
				lhs = (Column) a.getRightExpression();
				re = a.getLeftExpression();
			}
			PrimitiveValue rhs = null;
			if(re instanceof Function){
				Function fn = (Function)re;
				String fn_nm = ((Function) re).getName().toUpperCase();
				if(fn_nm.equals("DATE")){
					rhs = new DateValue(fn.getParameters().getExpressions().get(0).toString().replaceAll("'", ""));
				}
			}
			if(re instanceof DateValue){
				rhs = (DateValue)re;
			}
			else if(re instanceof LongValue){
				rhs = (LongValue)re;
			}
			else if(re instanceof DoubleValue){
				rhs = (DoubleValue)re;
			}
			else if(re instanceof StringValue){
				rhs = (StringValue)re;
			}
			else if(re instanceof BooleanValue){
				rhs = (BooleanValue)re;
			}
			
		    if(lhs == null || rhs == null) indexed = -1;
		    else{
		    	Table t = lhs.getTable();
		    	String col_nm = lhs.getColumnName();
		    	QIndex idx = table.s_index;
		    	if(table.hasIndex(col_nm)){
		    		idx_it = new IndexIterator(idx, col_nm, rhs, exp_type);
		    		indexed = 1;
		    	}
		    	else{
		    		tab_expr_it = new TabexperIterator(table, where_exp);
		    		indexed = 0;
		    	}
		    }
			
		}
		else if(where_exp instanceof MinorThan){
			MinorThan a = (MinorThan) where_exp;
			exp_type = 4;
			Column lhs; Expression re;
			if(a.getLeftExpression() instanceof Column){
				lhs = (Column) a.getLeftExpression();
				re = a.getRightExpression();
			}
			else{
				lhs = (Column) a.getRightExpression();
				re = a.getLeftExpression();
			}
			PrimitiveValue rhs = null;
			if(re instanceof Function){
				Function fn = (Function)re;
				String fn_nm = ((Function) re).getName().toUpperCase();
				if(fn_nm.equals("DATE")){
					rhs = new DateValue(fn.getParameters().getExpressions().get(0).toString().replaceAll("'", ""));
				}
			}
			if(re instanceof DateValue){
				rhs = (DateValue)re;
			}
			else if(re instanceof LongValue){
				rhs = (LongValue)re;
			}
			else if(re instanceof DoubleValue){
				rhs = (DoubleValue)re;
			}
			else if(re instanceof StringValue){
				rhs = (StringValue)re;
			}
			else if(re instanceof BooleanValue){
				rhs = (BooleanValue)re;
			}
			
		    if(lhs == null || rhs == null) indexed = -1;
		    else{
		    	Table t = lhs.getTable();
		    	String col_nm = lhs.getColumnName();
		    	QIndex idx = table.s_index;
		    	if(table.hasIndex(col_nm)){
		    		idx_it = new IndexIterator(idx, col_nm, rhs, exp_type);
		    		indexed = 1;
		    	}
		    	else{
		    		tab_expr_it = new TabexperIterator(table, where_exp);
		    		indexed = 0;
		    	}
		    }
			
		}
		else if(where_exp instanceof MinorThanEquals){
			MinorThanEquals a = (MinorThanEquals) where_exp;
			exp_type = 5;
			Column lhs; Expression re;
			if(a.getLeftExpression() instanceof Column){
				lhs = (Column) a.getLeftExpression();
				re = a.getRightExpression();
			}
			else{
				lhs = (Column) a.getRightExpression();
				re = a.getLeftExpression();
			}
			PrimitiveValue rhs = null;
			if(re instanceof Function){
				Function fn = (Function)re;
				String fn_nm = ((Function) re).getName().toUpperCase();
				if(fn_nm.equals("DATE")){
					rhs = new DateValue(fn.getParameters().getExpressions().get(0).toString().replaceAll("'", ""));
				}
			}
			if(re instanceof DateValue){
				rhs = (DateValue)re;
			}
			else if(re instanceof LongValue){
				rhs = (LongValue)re;
			}
			else if(re instanceof DoubleValue){
				rhs = (DoubleValue)re;
			}
			else if(re instanceof StringValue){
				rhs = (StringValue)re;
			}
			else if(re instanceof BooleanValue){
				rhs = (BooleanValue)re;
			}
			
		    if(lhs == null || rhs == null) indexed = -1;
		    else{
		    	Table t = lhs.getTable();
		    	String col_nm = lhs.getColumnName();
		    	QIndex idx = table.s_index;
		    	if(table.hasIndex(col_nm)){
		    		idx_it = new IndexIterator(idx, col_nm, rhs, exp_type);
		    		indexed = 1;
		    	}
		    	else{
		    		tab_expr_it = new TabexperIterator(table, where_exp);
		    		indexed = 0;
		    	}
		    }
			
		}

		
	}
	public boolean hasNext(){
		if(indexed==0){
			return tab_expr_it.hasNext();
		}
		else if(indexed==1){
			return idx_it.hasNext();
		}
		else{
			return false;
		}
	}
	
	public PrimitiveValue[] next(){
		if(indexed==0){
			return tab_expr_it.next();
		}
		else if(indexed==1){
			return idx_it.next();
		}
		else{
			return null;
		}
	}
	
	public PrimitiveValue getColVal(String col_name){
		
		if(indexed==1){
			return idx_it.getColVal(col_name);
		}
		else if(indexed==0){
			return tab_expr_it.getColVal(col_name);
		}	
		return null;
	}
	
	public PrimitiveValue getPrimVal(){

		if(indexed==1){
			return idx_it.getPrimVal();
		}
		else if(indexed==0){
			return tab_expr_it.getPrimVal();
		}	
		return null;
	}

}

