package dubstep;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Stack;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.schema.Column;

class AndORExpdata{
	int exptype;
	Expression expr;
	
	public AndORExpdata(Expression exp) {
		// TODO Auto-generated constructor stub
		expr = exp;
		exptype = -1;
	}
	public AndORExpdata(Expression exp, int expt) {
		// TODO Auto-generated constructor stub
		expr = exp;
		exptype = expt;
	}
}
class AndORExpEval2{
	
	Stack<AndORExpdata> Expr_stack = new Stack<>();
	QTable tab;
	Evaluator ev;
	PrimitiveValue[] cols;
	boolean hasVal=false;
	
	Eval e = new Eval() {
		
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			// TODO Auto-generated method stub
			int index = tab.t_mapper.get(arg0.getColumnName());
			return cols[index];
		}
	};
	
	private boolean hasIndex(Expression e){
		boolean hasInd = false;
		BinaryExpression be = (BinaryExpression)e;
		Expression l = be.getLeftExpression();
		Expression r = be.getRightExpression();
		if(l instanceof Column){
			Column c = (Column)l;
			hasInd = tab.hasIndex(c.getColumnName());
		}
		else if(l instanceof Column){
			Column c = (Column)r;
			hasInd = tab.hasIndex(c.getColumnName());
		}
		return hasInd;
	}
	
	public AndORExpEval2(QTable table, Expression exp) throws InvalidPrimitive, SQLException, IOException{

		tab = table;
		AndORExpdata data = new AndORExpdata(exp);
		Expr_stack.push(data);
		
		Expression top = Expr_stack.peek().expr;

		while(top instanceof AndExpression || top instanceof OrExpression){
			if(top instanceof AndExpression){
				AndExpression a = (AndExpression) top;
				Expression lhs = a.getLeftExpression();
				Expression rhs = a.getRightExpression();
				
//				System.out.println("LHS:"+lhs+" RHS:"+rhs);
				if(!(lhs instanceof AndExpression) && !(lhs instanceof OrExpression)){
					if(hasIndex(lhs)){
						AndORExpdata data1 = new AndORExpdata(lhs);
						AndORExpdata data2 = new AndORExpdata(rhs, 0);
						Expr_stack.pop();
						Expr_stack.push(data2);
						Expr_stack.push(data1);
					}
					else{
						AndORExpdata data1 = new AndORExpdata(rhs);
						AndORExpdata data2 = new AndORExpdata(lhs, 0);
						Expr_stack.pop();
						Expr_stack.push(data2);
						Expr_stack.push(data1);
					}
				}
				else{
					if(!(rhs instanceof AndExpression) && !(rhs instanceof OrExpression)){
						if(hasIndex(rhs)){
							AndORExpdata data1 = new AndORExpdata(rhs);
							AndORExpdata data2 = new AndORExpdata(lhs, 0);
							Expr_stack.pop();
							Expr_stack.push(data2);
							Expr_stack.push(data1);
						}
						else{
							AndORExpdata data1 = new AndORExpdata(lhs);
							AndORExpdata data2 = new AndORExpdata(rhs, 0);
							Expr_stack.pop();
							Expr_stack.push(data2);
							Expr_stack.push(data1);
						}
					}
					else{
						//Not implemented index
						AndORExpdata data1 = new AndORExpdata(lhs);
						AndORExpdata data2 = new AndORExpdata(rhs, 0);
						Expr_stack.pop();
						Expr_stack.push(data2);
						Expr_stack.push(data1);
					}
				}
			}
			else if(top instanceof OrExpression){
				//To be implemented
			}
			top = Expr_stack.peek().expr;
		}

		ev = new Evaluator(tab, Expr_stack.pop().expr);
	}
	
	public boolean hasNext() throws InvalidPrimitive, SQLException{
		if(hasVal){
			return true;
		}
		while(ev.hasNext()){
			boolean hasnextoncur = true;
			cols = ev.next();
			for(int i=Expr_stack.size()-1; i>=0; i--){
				AndORExpdata data = Expr_stack.get(i);
				if(!e.eval(data.expr).toBool()){
					hasnextoncur = false;
					break;
				}
			}
			if(hasnextoncur){
				hasVal = true;
				return true;
			}
		}
		return false;
	}
	
	public PrimitiveValue[] next(){
		hasVal = false;
		return cols;
	}

	public PrimitiveValue getColVal(String col_name){

		int index = tab.t_mapper.get(col_name);
		
		return cols[index];
	}
	public PrimitiveValue getPrimVal(){
		String Prim_val="";

		for(int i = 0; i<tab.prim_nm.size(); i++){
			
			int index = tab.t_mapper.get(tab.prim_nm.get(i));
			Prim_val += cols[index];
		}
		
		return new StringValue(Prim_val);
	}
	
	
	
}
class AndORExpEval{
	
	Expression andor1;
	Expression andor2;
	Expression eval1;
	Expression eval2;
	int andor_exp_type;
	
	AndORExpEval a1;
	AndORExpEval a2;
	Evaluator evr;
	boolean hasVal = false;

	boolean hasIndex=false;
	QTable tab;
	PrimitiveValue[] cols;

	Eval e = new Eval() {
		
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			// TODO Auto-generated method stub
			int index = tab.t_mapper.get(arg0.getColumnName());
			return cols[index];
		}
	};
	public AndORExpEval(QTable table, Expression exp) throws InvalidPrimitive, SQLException, IOException {
		// TODO Auto-generated constructor stub
		
		tab = table;
		
		if(exp instanceof AndExpression){
			AndExpression and_exp = (AndExpression)exp;
			Expression lhs = and_exp.getLeftExpression();
			Expression rhs = and_exp.getRightExpression();
			
			if(lhs instanceof AndExpression || lhs instanceof OrExpression){
				andor1 = lhs;
				a1 = new AndORExpEval(table, andor1);
				if(rhs instanceof AndExpression || rhs instanceof OrExpression){
					andor2 = rhs;
					a2 = new AndORExpEval(table, andor2);
					hasIndex = a1.hasIndex || a2.hasIndex;
					andor_exp_type = 4;
				}
				else{
					eval2 = rhs;
					a2 = new AndORExpEval(table, eval2);
					hasIndex = a1.hasIndex || a2.hasIndex;
					andor_exp_type = 1;
				}
			}
			else{
				eval1 = lhs;
				if(rhs instanceof AndExpression || rhs instanceof OrExpression){
					andor2 = rhs;
					a1 = new AndORExpEval(table, eval1);
					a2 = new AndORExpEval(table, andor2);
					andor_exp_type = 2;
				}
				else{
					eval2 = rhs;
					BinaryExpression b1 = (BinaryExpression) lhs; 
					Column c1 = (Column) b1.getLeftExpression(); 
					BinaryExpression b2 = (BinaryExpression) rhs; 
					Column c2 = (Column) b2.getLeftExpression(); 
					if(table.hasIndex(c1.getColumnName())){
						a1 = new AndORExpEval(table, eval1);
						eval1 = eval2;
					}
					else if(table.hasIndex(c2.getColumnName())){
						a1 = new AndORExpEval(table, eval2);
					}
					else{
						a1 = new AndORExpEval(table, eval1);
						eval1 = eval2;
					}
					andor_exp_type = 3;
				}
			}
		}
		else if(exp instanceof OrExpression){
			OrExpression and_exp = (OrExpression)exp;
			Expression lhs = and_exp.getLeftExpression();
			Expression rhs = and_exp.getRightExpression();
			
			if(lhs instanceof AndExpression || lhs instanceof OrExpression){
				andor1 = lhs;
				if(rhs instanceof AndExpression || rhs instanceof OrExpression){
					andor2 = rhs;
					andor_exp_type = 8;
					System.out.println(andor_exp_type+"("+lhs+")OR("+rhs+")");
					a1 = new AndORExpEval(table, andor1);
					a2 = new AndORExpEval(table, andor2);
				}
				else{
					eval2 = rhs;
					andor_exp_type = 5;
					System.out.println(andor_exp_type+"("+lhs+")OR("+rhs+")");
					a1 = new AndORExpEval(table, andor1);
					a2 = new AndORExpEval(table, eval2);
				}
			}
			else{
				eval1 = lhs;
				if(rhs instanceof AndExpression || rhs instanceof OrExpression){
					andor_exp_type = 6;
					System.out.println(andor_exp_type+"("+lhs+")OR("+rhs+")");
					andor2 = rhs;
					a1 = new AndORExpEval(table, eval1);
					a2 = new AndORExpEval(table, andor2);
				}
				else{
					eval2 = rhs;
					andor_exp_type = 7;
					System.out.println(andor_exp_type+"("+lhs+")OR("+rhs+")");
					a1 = new AndORExpEval(table, eval1);
					a2 = new AndORExpEval(table, eval2);
				}
			}
		}
		else{
			andor_exp_type = 0;
			evr = new Evaluator(table, exp);
			System.out.println("Came here??@andor_exp_type=0");
			System.out.println(andor_exp_type+"("+exp+")");
		}
	}
	
	public boolean hasNext() throws InvalidPrimitive, SQLException{
		if(hasVal){
			return true;
		}
		switch(andor_exp_type){
		case 0:
			if(evr.hasNext()){
				cols = evr.next();
				hasVal = true;
			}
			return hasVal;
		case 1:
			while(a2.hasNext()){
				cols = a2.next();
				if(e.eval(andor1).toBool()){
					hasVal = true;
					return hasVal;
				}
			}
			return hasVal;
		case 2:
			while(a1.hasNext()){
				cols = a1.next();
				if(e.eval(andor2).toBool()){
					hasVal = true;
					return hasVal;
				}
			}
			return hasVal;
		case 3:
			while(a1.hasNext()){
				cols = a1.next();
				if(e.eval(eval1).toBool()){
					hasVal = true;
					return hasVal;
				}
			}
			return hasVal;
		case 4:
			while(a1.hasNext()){
				cols = a1.next();
				if(e.eval(andor2).toBool()){
					hasVal = true;
					return hasVal;
				}
			}
			return hasVal;
		case 5:
			if(a2.hasNext()){
				cols = a2.next();
				hasVal = true;
				return hasVal;
			}
			else if(a1.hasNext()){
				while(a1.hasNext()){
					cols = a1.next();
					if(!e.eval(eval2).toBool()){
						hasVal = true;
						return hasVal;
					}
				}
			}
			return hasVal;
		case 6:
			if(a1.hasNext()){
				cols = a1.next();
				hasVal = true;
				return hasVal;
			}
			else if(a2.hasNext()){
				while(a2.hasNext()){
					cols = a2.next();
					if(!e.eval(eval1).toBool())
						hasVal = true;
						return hasVal;
				}
			}
			return hasVal;
		case 7:
			if(a1.hasNext()){
				cols = a1.next();
				hasVal = true;
				return hasVal;
			}
			else if(a2.hasNext()){
				while(a2.hasNext()){
					cols = a2.next();
					if(!e.eval(eval1).toBool()){
						hasVal = true;
						return hasVal;
					}
				}
			}
			return hasVal;
		case 8:
			if(a1.hasNext()){
				cols = a1.next();
				hasVal = true;
				return hasVal;
			}
			else if(a2.hasNext()){
				while(a2.hasNext()){
					cols = a2.next();
					if(!e.eval(andor1).toBool()){
						hasVal = true;
						return hasVal;
					}
				}
			}
			return hasVal;
		default:
			return false;
		}
	}
	
	public PrimitiveValue[] next() throws InvalidPrimitive, SQLException{
		hasVal=false;
		return cols;
	}
	
	public PrimitiveValue getColVal(String col_name){

		int index = tab.t_mapper.get(col_name);
		
		return cols[index];
	}
	public PrimitiveValue getPrimVal(){
		String Prim_val="";

		for(int i = 0; i<tab.prim_nm.size(); i++){
			
			int index = tab.t_mapper.get(tab.prim_nm.get(i));
			Prim_val += cols[index];
		}
		
		return new StringValue(Prim_val);
	}

}
