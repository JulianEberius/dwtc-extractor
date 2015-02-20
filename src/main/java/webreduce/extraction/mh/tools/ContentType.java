package webreduce.extraction.mh.tools;

//the order within the enum determines the priority:
// the first values are preferred to the last ones in
// case of dominant type determination with equal
// occurrence counts
public enum ContentType {
	FORM,
	HYPERLINK,
	IMAGE,
	ALPHABETICAL,
	DIGIT,
	EMPTY,
	OTHERS
}
		
