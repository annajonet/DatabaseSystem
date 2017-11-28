package dubstep;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.util.ArrayList;


import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;

class datapoints{
	int pos;
	int len;
	
	public datapoints(int p, int l){
		pos = p;
		len = l;
	}
}

public class ReadFile {

	public static int EOL = System.lineSeparator().length();

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String tab_loc = "data/manutdpipe.csv";
		FileInputStream fsr = new FileInputStream(tab_loc);
		BufferedReader br = new BufferedReader(new InputStreamReader(fsr));  //new FileReader(tab_loc));
		
		String recrd;
//		StringBuffer recrd;
		
		
		long startTime = System.currentTimeMillis();
		
		int file_strt = 0;
		
		
		ArrayList<datapoints> loc = new ArrayList<>();
		
		
		String[] cols = new String[6];
		while((recrd = br.readLine())!=null){

			int len = recrd.length();
			System.out.println("Line Length:"+len);
			
			datapoints dp = new datapoints(file_strt, len);
			
			loc.add(dp);
			
			file_strt +=len+EOL; 
			System.out.println("Line Start:"+file_strt);
			

			int from_idx=0, cur_idx=0, incrt =0;

			while ((cur_idx = recrd.indexOf('|', from_idx)) != -1 && incrt<5) {
				cols[incrt++] = recrd.substring(from_idx, cur_idx);
				from_idx = cur_idx+1;
			}
			cols[incrt] = recrd.substring(from_idx);
		}
	
		
		BufferedReader ra = new BufferedReader(new InputStreamReader(new FileInputStream(tab_loc)));  //new FileReader(tab_loc));
		int prev_pos = 0;
		for(int i=0; i<loc.size(); i++){
			//read thru
			ra.skip(loc.get(i).pos-prev_pos);
			String rec = ra.readLine();
			System.out.println(rec);
			prev_pos = loc.get(i).pos+rec.length()+EOL;
			
		}
		ra.close();
		
/*		RandomAccessFile ra = new RandomAccessFile(tab_loc, "r");
		
		for(int i = 0; i<loc.size(); i=i+10){
			ra.seek(loc.get(i).pos);
			String rec = ra.readLine();
//			System.out.println("Record:"+rec);
		}
		ra.close();
*/		
		long endTime = System.currentTimeMillis();
		System.out.println("\nExecution Time:"+(endTime - startTime) + " ms");

		br.close();
	}

}
