package dubstep;

import java.util.Comparator;

public class FileListComparator  implements Comparator {
	 
	public int compare(Object obj1, Object obj2) {
		Limit index1 = (Limit) obj1;
		Limit index2 = (Limit) obj2;

	    /*int nameComp;*/
		for(int i=0; i<index1.getValue().length; i++){
			int returnVal = PrimitiveComparator.compareValue(index1.getValue()[i],index2.getValue()[i]);
			if(returnVal != 0){
				return returnVal;
			}
		}
	    return 0;

	}

}
