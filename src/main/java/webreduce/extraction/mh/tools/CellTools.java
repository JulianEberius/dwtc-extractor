package webreduce.extraction.mh.tools;

import org.apache.commons.lang3.StringEscapeUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.safety.Whitelist;

import com.google.common.base.CharMatcher;

public class CellTools {

	
		// returns the ContentType a cell contains
		// the different types are exclusive herein!
		public static ContentType getContentType(Element cellContent) {
			
			// Tags are NOT valued equally, <form> takes priority
			// and <img> is the least important
			// e.g. an image within a form is most likely an icon or visual hint
			// e.g. an image within in an anchor tag is most likely the link's
			// visual representation
			// e.g. an anchor within a form is mostly likely a link to a help page
			// or something related to the form's input fields (forgot password link)
			if (cellContent.getElementsByTag("form").size() > 0) {
				return ContentType.FORM;
			} else if (cellContent.getElementsByTag("a").size() > 0) {
				return ContentType.HYPERLINK;
			} else if (cellContent.getElementsByTag("img").size() > 0) {
				return ContentType.IMAGE;
			} else {
				// no relevant tags -> inspect content
				
				// clean and replace all invisible characters (this removes white spaces!)
				String cellStr = cleanCell(cellContent.text()).replaceAll("\\s+","");
			
				if (cellStr.length() > 0) {
					
					// count occurrences of alphabetical and numerical
					// characters within the content string
					int alphaCount = 0, digitCount = 0;
					for (char c : cellStr.toCharArray()) {
						if(Character.isAlphabetic(c)) {
							alphaCount++;
						} else if (Character.isDigit(c)) {
							digitCount++;
						}
					}
					
					if ((alphaCount + digitCount) == 0) {
						// neither alphabetical nor numerical
						return ContentType.OTHERS;
					} else {
						
						// determine dominant type						
						if (digitCount > alphaCount) {
							return ContentType.DIGIT;
						} else {
							return ContentType.ALPHABETICAL;
						}
					}
					
				} else {
					// empty string
					return ContentType.EMPTY;
				}
			}
		}
		
		// returns the cell content's length for a given cell
		// used for features which calculate results using this value
		// all cleaning should be done herein
		public static int getCellLength(Element cell) {
			return cell.text().length();
		}
		
		public static boolean isNumericOnly(String str)  
		{  
		  try  
		  {  
		    Double.parseDouble(str);  
		  }  
		  catch(NumberFormatException nfe)  
		  {  
		    return false;  
		  }  
		  return true;  
		}
		
		// cleans up cell's string content using JSoup
		public static String cleanCell(String cell) {
			cell = Jsoup.clean(cell, Whitelist.simpleText());
			cell = StringEscapeUtils.unescapeHtml4(cell);
			cell = CharMatcher.WHITESPACE.trimAndCollapseFrom(cell, ' ');
			return cell;
		}
}
