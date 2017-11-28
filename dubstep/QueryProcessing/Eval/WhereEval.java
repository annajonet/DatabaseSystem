package dubstep;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Stack;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;


public class WhereEval {
	String table_name;
	boolean hasExp = false;
	TableIterator tab_it;
	AndORExpEval2 andor_exp;
	
	public WhereEval(QTable table, Expression where_exp) throws InvalidPrimitive, SQLException, IOException{
		if(where_exp!=null){
			hasExp = true;
			andor_exp = new AndORExpEval2(table, where_exp);  //Choose AND OR Implementation....
		}
		else{	
			hasExp = false;
			tab_it = new TableIterator(table);
		}
	}
	
	public boolean hasNext() throws InvalidPrimitive, SQLException{
		if(hasExp){
			return andor_exp.hasNext();
		}
		else{
			return tab_it.hasNext();
		}
	}
	
	public PrimitiveValue[] next() throws InvalidPrimitive, SQLException{
		if(hasExp){
			return andor_exp.next();
		}
		else{
			return tab_it.next();
		}
		
	}
	
	public PrimitiveValue getColVal(String col_name){
		
		if(hasExp){
			return andor_exp.getColVal(col_name);
		}
		else{
			return tab_it.getColVal(col_name);
		}
	}
	
	public PrimitiveValue getPrimVal(){

		if(hasExp){
			return andor_exp.getPrimVal();
		}
		else{
			return tab_it.getPrimVal();
		}
	}
}

