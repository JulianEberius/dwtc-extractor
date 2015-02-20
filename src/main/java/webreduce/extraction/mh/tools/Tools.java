package webreduce.extraction.mh.tools;

import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.JFileChooser;
import javax.swing.filechooser.FileNameExtensionFilter;

import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class Tools {
	
	// creates an Instance object from feature calculation results
	// which is bound to an artificial training set so it can be
	// handed over to the classifier
	// (it might be useful to create a singleton Instances object
	// if this is called multiple times in row and just keep a counter
	// variable which tracks the index of the Instance placed into
	// the container so that the creation overhead of the Instances
	// object is reduced)
	public static Instance createInstanceFromData(HashMap<String, Double> featureResults, ArrayList<Attribute> attributes, FastVector attributesAsVector) {
		
		// unfortunately WEKA doesn't allow for standalone Instance objects without a dataset container
		// that's why we need to keep a FastVector of Attributes to create an Instances object
		
		Instances dataset = new Instances("TestDataset", attributesAsVector, 0);
//		System.out.println("Dataset created with " + attributesAsVector.size() + " expected attributes (including class) ...");
		dataset.setClassIndex(dataset.numAttributes() - 1); // last attribute is classAttr
		
		// +1 because of additional class attribute
		Instance resultInstance = new Instance(attributes.size() + 1);

		for (int i = 0; i < attributes.size(); i++) {
			String featureName = attributes.get(i).name();
			resultInstance.setValue(i, featureResults.get(featureName));
		}
//		System.out.println("Test instance filled with " + attributes.size() + " values (excluding class) ...");
		
		// add a dummy class attribute value
		resultInstance.setValue(dataset.classAttribute(), -1);
		
		dataset.add(resultInstance);
		
//		System.out.println("Created instance has " + dataset.firstInstance().numClasses() + " classes.");
		
		// return Instance connected to dataset
		// ('return resultInstance' won't work!)
		return dataset.firstInstance();
	}
	
	// writes instances data to ARFF file
	public static void writeInstancesToDisk(String path, Instances data) throws Exception {

			System.out.println("Writing " + path);
			weka.core.converters.ConverterUtils.DataSink.write(path, data);

	}
	
	// displays file chooser for selecting an ARFF and returns path if selected
	// otherwise null
	public static String chooseArff() {
		JFileChooser arffChooser = new JFileChooser();
		FileNameExtensionFilter filter = new FileNameExtensionFilter("ARFF files", "arff");
		arffChooser.setFileFilter(filter);
		if (arffChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
			return arffChooser.getSelectedFile().getPath();
		} else return null;
		
	}
	
	// creates a new set of Instances which only contain the class attribute plus the attribute 'attr'
	public static Instances singleOutAttribute(Instances inst, Attribute attr) {
		Instances result = new Instances(inst);
		for (int i = result.numAttributes() - 1; i >= 0; i--) {
			Attribute temp = result.attribute(i);
			if (temp.name().equals(attr.name()) || temp == result.classAttribute()) continue;
			result.deleteAttributeAt(i);
		}
		return result;
	}
	
	// creates a new set of Instances which only contains the class attribute plus the attributes
	// listed in 'attrList'
	public static Instances singleOutAttributes(Instances inst, ArrayList<Attribute> attrList) {
		ArrayList<String> names = new ArrayList<String>();
		for (Attribute attr : attrList) {
			names.add(attr.name());
		}
		Instances result = new Instances(inst);
		for (int i = result.numAttributes() - 1; i >= 0; i--) {
			Attribute temp = result.attribute(i);
			if (names.contains(temp.name()) || temp == result.classAttribute()) continue;
			result.deleteAttributeAt(i);
		}
		return result;
	}
	
	
}
