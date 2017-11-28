package dubstep;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeMap;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.PrimitiveValue.InvalidPrimitive;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

public class QueryProcessor {
	
	class joiner{
		String alias_name;   //ch
		HashMap<String, Expression> jn_exp = new HashMap<>(); // implement expression list 
		ArrayList<Expression> tb_exp = new ArrayList<>();
		boolean is_alias; //ch
		
		public joiner(String name, boolean flag) {
			// TODO Auto-generated constructor stub
			alias_name = name; //ch
			is_alias = flag; //ch
		}
	}
	
	String q_name;
	String q_alias;
	PlainSelect req_query;
	FromItem from;
	Expression where_exp;
	ArrayList<OrderByElement> order_by;
	List<SelectItem> projections;
	ArrayList<Column> group_cols;
	Limit limiter;
	LinkedHashMap<String, Integer> Qp_mapper = new LinkedHashMap<>();
	HashMap<String, QTable> table_map= new HashMap<>();
	WhereEval wh_ev;
	PrimitiveValue[] cols;
	ArrayList<Expression> prj_expr = new ArrayList<>();
	int n_attr;
	int n_queryargs;
	boolean hasAgg = false;
	boolean istablevel;
	int qtype = -1;
	QueryProcessor subq;
	boolean hasVal = false;
	long n_limit;
	Projection proj;
	ArrayList<Join> join_list =  new ArrayList<>();
	ArrayList<QueryProcessor> join_qp = new ArrayList<>();
	LinkedHashMap<String, joiner> join_def = new LinkedHashMap<>();
	ArrayList<ArrayList<Integer>> Join_index_list = new ArrayList<>();
	Join2 QJoin;
	Expression rem_where_exp = null; 
	
	//added for order by
	ArrayList<Expression> order_expr = null;
	
	Eval e = new Eval() {
		
		@Override
		public PrimitiveValue eval(Column arg0) throws SQLException {
			// TODO Auto-generated method stub
			int index = 0;
			try{
				if(join_list==null)
					index = Qp_mapper.get(arg0.getColumnName());
				else{
					if(arg0.getTable().getName()==null)
						index = Qp_mapper.get(arg0.getColumnName());
					else
						index = Qp_mapper.get(arg0.getTable().getName()+"_"+arg0.getColumnName());
				}
			}
			catch(Exception e){
			}

			return cols[index];

		}
	};

	public QueryProcessor(SelectBody sel_q, HashMap<String, QTable> table_mapper) throws SQLException, IOException, ParseException{
		if(sel_q instanceof PlainSelect){
			 req_query = (PlainSelect) sel_q;
		}
		else{
			throw new java.sql.SQLException("ERR: Select Query unknown :"+sel_q);
		}
		
		table_map = table_mapper;
		
		from = req_query.getFromItem();
		where_exp = req_query.getWhere();
		order_by = (ArrayList<OrderByElement>) req_query.getOrderByElements();
		group_cols = (ArrayList<Column>) req_query.getGroupByColumnReferences();
		limiter = req_query.getLimit();
		projections = req_query.getSelectItems();
		join_list = (ArrayList<Join>) req_query.getJoins();
		if(limiter!=null)
			n_limit = limiter.getRowCount();
		
		if(join_list != null){
			istablevel = false;
			qtype = 2;
			String[] join_tab_q = new String[join_list.size()+1];
			
			String col_alias = null; //ch
			if(from.getAlias()!=null) //ch
				col_alias = from.getAlias(); //ch
			join_def.put(((Table) from).getName(), new joiner(col_alias, false)); // ch
			if(from.getAlias()!=null) //ch
				join_def.put(col_alias, new joiner(((Table) from).getName(), true)); //ch
			//ch
			//ch
			
			for(int i=0; i<join_list.size(); i++){
				Join jn_tab = join_list.get(i);
				//alias not added
//				System.out.println("join-->"+jn_tab.toString());
				String join_nm = jn_tab.toString(); //ch
				String[] join_nms = join_nm.split(" AS "); //ch
				if(join_nms.length<2){ //ch
					join_def.put(join_nm, new joiner(null, false)); //ch
				} //ch
				else{ //ch
					join_def.put(join_nms[0], new joiner(join_nms[1], false)); //ch
					join_def.put(join_nms[1], new joiner(join_nms[0], true)); //ch
				}
			}
			
			ArrayList<Expression> exp_list = new ArrayList<>();
			Expression exprn = where_exp;
			while(exprn != null){
				if(exprn instanceof AndExpression){ // OR not handled
					AndExpression exp = (AndExpression)exprn;
					exp_list.add(exp.getRightExpression());
					exprn = exp.getLeftExpression();
				}
				else{
					exp_list.add(exprn);
					exprn = null;
				}
			}
//			System.out.println("Where Coditions:"+exp_list);

			for(int i=0; i<exp_list.size(); i++){
				Expression exp = exp_list.get(i);
				if(exp instanceof BinaryExpression){
					BinaryExpression bi_exp = (BinaryExpression)exp;
					if(bi_exp instanceof OrExpression){
						if(rem_where_exp==null){
							rem_where_exp = exp;
						}
						else{
							rem_where_exp = new AndExpression(rem_where_exp, exp);
						}
					}
					else{
						Column lhs; Expression re;
						if(bi_exp.getLeftExpression() instanceof Column){
							lhs = (Column) bi_exp.getLeftExpression();
							re = bi_exp.getRightExpression();
						}
						else{
							lhs = (Column) bi_exp.getRightExpression();
							re = bi_exp.getLeftExpression();
						}
						if(re instanceof Column){
							Column rhs = (Column)re;
							if(join_def.containsKey(lhs.getTable().getName())){
								joiner jd = join_def.get(lhs.getTable().getName());
								if(jd.is_alias){ //ch
									jd = join_def.get(jd.alias_name); //ch
								} //ch
								if(lhs.getTable().getName().equals(rhs.getTable().getName()))
									jd.tb_exp.add(exp);
								else
									jd.jn_exp.put(rhs.getTable().getName(), exp);
							}
							if(join_def.containsKey(rhs.getTable().getName())){
								joiner jd = join_def.get(rhs.getTable().getName());
								if(jd.is_alias){ //ch
									jd = join_def.get(jd.alias_name); //ch
								} //ch
								if(!lhs.getTable().getName().equals(rhs.getTable().getName()))
									jd.jn_exp.put(lhs.getTable().getName(), exp);
							}
						}
						else{
							if(join_def.containsKey(lhs.getTable().getName())){
								joiner jd = join_def.get(lhs.getTable().getName());
								if(jd.is_alias){ //ch
									jd = join_def.get(jd.alias_name); //ch
								} //ch
								jd.tb_exp.add(exp);
							}
							else{
								if(rem_where_exp==null){
									rem_where_exp = exp;
								}
								else{
									rem_where_exp = new AndExpression(rem_where_exp, exp);
								}
							}
						}
					}
				}
				else{
					if(rem_where_exp==null){
						rem_where_exp = exp;
					}
					else{
						rem_where_exp = new AndExpression(rem_where_exp, exp);
					}
				}
				
			}
			
			int join_column_offset = 0;
			Iterator<String> join_it = join_def.keySet().iterator();
			ArrayList<String> temp_joined_list = new ArrayList<>();
			while(join_it.hasNext()){
				String tab = join_it.next();
				joiner jn = join_def.get(tab);
				if(!jn.is_alias){ //ch
					ArrayList<Expression> table_wh_exp = jn.tb_exp;
					String j_sel = "SELECT * FROM "+tab;
					if(jn.alias_name!=null) //ch
						j_sel += " AS "+jn.alias_name;//ch
					if(table_wh_exp.size()>0)
						j_sel +=" WHERE ";
					for(int i=0; i<table_wh_exp.size(); i++){
						j_sel += table_wh_exp.get(i);
						if(i==table_wh_exp.size()-1){
							j_sel +=";";
						}
						else{
							j_sel += " AND ";
						}
					}
					
//					System.out.println("Query "+j_sel);

					CCJSqlParser parser = new CCJSqlParser(new StringReader(j_sel));
					Statement query = parser.Statement();
					Select jn_sel = (Select)query;
					QueryProcessor qp = new QueryProcessor(jn_sel.getSelectBody(), table_mapper);
					Iterator<String> qp_map_it = qp.Qp_mapper.keySet().iterator();
					while(qp_map_it.hasNext()){
						String colmn = qp_map_it.next();
						int index = join_column_offset+qp.Qp_mapper.get(colmn);
						this.Qp_mapper.put(colmn, index);
						colmn = qp.q_name+"_"+colmn;
						this.Qp_mapper.put(colmn, index);
						if(qp.q_alias!=null){
							colmn = qp.q_alias+"_"+colmn;
							this.Qp_mapper.put(colmn, index);
						}
					}

					Iterator<String> join_it1 = join_def.keySet().iterator();
					while(join_it1.hasNext()){
						String tab1 = join_it1.next();
						joiner jn1 = join_def.get(tab);
//						System.out.println(tab+" --> "+jn1.jn_exp+"--"+jn1.tb_exp);
					}

					if(!temp_joined_list.isEmpty()){
						ArrayList<Integer> joins = new ArrayList<>();
						for(int i=0; i< temp_joined_list.size(); i++){
							String tab_nm = temp_joined_list.get(i);
							if(jn.jn_exp.containsKey(tab_nm)){
								int index1,index2;
								Expression exp = jn.jn_exp.get(tab_nm);
								BinaryExpression bi_exp = (BinaryExpression)exp;
								Column c1 = (Column)bi_exp.getLeftExpression();
								Column c2 = (Column)bi_exp.getRightExpression();
								if(c1.getTable().getName().equals(tab)){
									String col_nm = tab_nm+"_"+c2.getColumnName();
									index1 = Qp_mapper.get(col_nm);
									index2 = qp.Qp_mapper.get(c1.getColumnName());
								}
								else{
									String col_nm = tab_nm+"_"+c1.getColumnName();
									index1 = Qp_mapper.get(col_nm);
									index2 = qp.Qp_mapper.get(c2.getColumnName());
								}
								joins.add(index1);
								joins.add(index2);
							}
						}
//						System.out.println(joins);
						Join_index_list.add(joins);
					}
					QTable t = qp.table_map.get(tab);
					join_column_offset += t.n_attr; 
					join_qp.add(qp);
					temp_joined_list.add(tab);
				}
			}
//			System.out.println("Join Condition"+Join_index_list);
//			System.out.println("Temp_Joined_list"+temp_joined_list);
//			System.out.println("QP_Mapper:"+Qp_mapper);
/*			System.out.print("Qp_list:[");
			for(int i=0; i<join_qp.size(); i++){
				System.out.print(join_qp.get(i).q_name+" ");
			}
			System.out.println("]");
*/		}
		// no joins
		else{
			if(from instanceof Table){
				qtype = 0;
				istablevel = true;
				Table table = (Table) from;
	//			String Tab_nm = tab.getName();
				QTable tab = table_map.get(table.getName());
	//			System.out.println("Table Name:"+table.getName());
				wh_ev = new WhereEval(tab, where_exp);
				Qp_mapper = tab.t_mapper;
				n_attr = tab.n_attr;
				q_name = table.getName();
				q_alias = table.getAlias();
			}
			else if(from instanceof SubSelect){
				istablevel = false;
				qtype = 1;
				q_alias = from.getAlias();
				SubSelect sub_sel = (SubSelect)from;
				PlainSelect sel_sp;
				if(sub_sel.getSelectBody() instanceof PlainSelect){
					sel_sp = (PlainSelect) sub_sel.getSelectBody();
				}
				else{
					throw new java.sql.SQLException("ERR: Select Query unknown :"+sub_sel);
				}
				ArrayList<SelectItem> S_attr_list = (ArrayList<SelectItem>) sel_sp.getSelectItems();
				subq = new QueryProcessor(sel_sp, table_mapper);
				for(int i=0; i<S_attr_list.size(); i++){
					SelectItem attr = S_attr_list.get(i);
					if(attr instanceof SelectExpressionItem){
						SelectExpressionItem attr_exp = (SelectExpressionItem) attr;
						if(attr_exp.getExpression() instanceof Column){
							Column c = (Column) attr_exp.getExpression();
							Qp_mapper.put(c.getColumnName(), i);
						}
						if(attr_exp.getAlias() != null){
							Qp_mapper.put(attr_exp.getAlias(), i);
						}
					}
					else if(attr instanceof AllColumns){
						Qp_mapper = subq.Qp_mapper;
					}
				}
				n_attr = Qp_mapper.keySet().size();
			}
		}
		for(int i=0; i<projections.size(); i++){
			SelectItem sel_item = projections.get(i);
			if(sel_item instanceof SelectExpressionItem){
				SelectExpressionItem sel_expr = (SelectExpressionItem) sel_item;
				if(sel_expr.getExpression() instanceof Function){
					hasAgg = true;
				}
				prj_expr.add(sel_expr.getExpression());
			}
			else if(sel_item instanceof AllColumns){
				Iterator<String> collist_it = Qp_mapper.keySet().iterator();
				while(collist_it.hasNext()){
					prj_expr.add(new Column(new Table(from.getAlias()), collist_it.next())); 
				}
			}
		}
		proj = new Projection(this);
		n_queryargs = prj_expr.size();
	}
	
	public PrimitiveValue[] readRecord() throws SQLException{

		PrimitiveValue[] col_val = new PrimitiveValue[n_queryargs];

		if(hasAgg||group_cols!=null){
			col_val = cols;
		}
		else{
			col_val = proj.Project(cols);
		}
		hasVal = false;
		return col_val;

	}
	
	public boolean hasReadRecord() throws InvalidPrimitive, SQLException{
		if(hasVal){
			return hasVal;
		}
		if(qtype==0){
			if(wh_ev.hasNext()){
				cols = wh_ev.next();
				hasVal = true;
			}
			return hasVal;
		}
		else if(qtype==1){
			while(subq.hasReadRecord()){
				cols = subq.readRecord();
				if(where_exp!=null && !e.eval(where_exp).toBool()){
					hasVal = false;
					continue;
				}
				hasVal = true;
				return hasVal;
			}
			return false;
		}
		else if(qtype==2){
			while(QJoin.hasNext()){
				cols = QJoin.next();
				if(rem_where_exp != null && !e.eval(rem_where_exp).toBool()){
					hasVal = false;
					continue;
				}
				hasVal = true;
				return hasVal;
			}
			return false;
		}
		else{
			return false;
		}
	}

	public void readQuery() throws SQLException, IOException{
		PrimitiveValue[] colmns;

		Sort sorter = null;
		boolean noRecords = true;
		
		if(join_list != null){
			QJoin = new Join2(Join_index_list);
			QJoin.addData(join_qp);
		}
//		Join_operation *******************************************************************->>>>

		while(hasReadRecord()){
			noRecords = false;
			if(hasAgg||group_cols!=null){
				
				GroupBy grp = new GroupBy(this);
				while(hasReadRecord()){
					cols = readRecord();
					grp.addData(cols);
				}
				
				
				while(grp.gb_hasNext()){
					
					colmns = grp.gb_Next();
					colmns = Arrays.copyOfRange(colmns, 0, colmns.length-1);
					if(order_by!=null){
						if(sorter == null){
							int extra_cols = (order_expr == null)?0:order_expr.size();
							sorter = new Sort(grp.gb_mapper, order_by, extra_cols, join_list != null);
						}
						sorter.addData(colmns);
					}
					else{
						String row_res = "";
						if(colmns==null){
							for(int i=0; i<n_queryargs; i++){
								row_res += "null|";
							}
						}
						else{
							for(int i=0; i<n_queryargs; i++){
								row_res += colmns[i]+"|";
							}
						}
						if(row_res.length()>n_queryargs){
							System.out.println(row_res.substring(0, row_res.length()-1));
							if(limiter!=null)
								n_limit--;
						}
					}
					if(limiter!=null && n_limit==0) break;
				}
			}
			else{
				colmns = readRecord();
				if(order_by!=null){
					if(sorter == null){
						sorter = new Sort(Qp_mapper, order_by, 0, join_list != null);
					}
					sorter.addData(cols);
				}
				else{
					String row_res = "";
					if(colmns==null){
						for(int i=0; i<n_queryargs; i++){
							row_res += "null|";
						}
					}
					else{
						for(int i=0; i<n_queryargs; i++){
							row_res += colmns[i]+"|";
						}
					}
					if(row_res.length()>n_queryargs){
						System.out.println(row_res.substring(0, row_res.length()-1));
						if(limiter!=null)
							n_limit--;
					}
					
				}
				if(limiter!=null && n_limit==0) break;
			}
		}
		if(order_by!=null){
			if(sorter != null){
				sorter.sort();
				while(sorter.hasNext()){
					if(hasAgg||group_cols!=null){
						System.out.println(sorter.next());
					}else{
						String nextRec = sorter.next();
						String[] recs = nextRec.split("\\|");
						String outVal = "";
						for(int i=0; i<projections.size(); i++){
							SelectItem sel_item = projections.get(i);
							if(sel_item instanceof SelectExpressionItem){
								SelectExpressionItem attr_exp = (SelectExpressionItem) sel_item;
								if(attr_exp.getExpression() instanceof Column){
									Column c = (Column) attr_exp.getExpression();
									outVal += recs[Qp_mapper.get(c.getColumnName())]+"|";
								}
								if(attr_exp.getAlias() != null){
									outVal += recs[Qp_mapper.get(attr_exp.getAlias())]+"|";
								}
								
							}
							else if(sel_item instanceof AllColumns){
								outVal = nextRec+"|";
								break;
							}
							
						}
						System.out.println(outVal.substring(0, outVal.length()-1));
					}
					n_limit--;
					if(n_limit==0) break;
				}
			}
		}
		if(noRecords){
			String outVal = "";
			if(hasAgg && (group_cols == null)){				
				for(int j=0;j<n_queryargs;j++){
					if(prj_expr.get(j) instanceof Function){
						if(prj_expr.get(j).toString().toUpperCase().substring(0, 3).equals("COU")){
							outVal += "0|";
						}else{
							outVal += "|";
						}
					}else{
						outVal += "|";
					}
				}
				System.out.println(outVal.substring(0,outVal.length()-1));
			}			
		}

	}
}