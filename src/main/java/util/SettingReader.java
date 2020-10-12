package util;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;


public class SettingReader {
    /**
     * funzione chamata da chi ha bisogno di leggere il file e che restituir� l'elemento passato come parametro
     * @param fileName il nome del file da leggere
     * @param tagName il tag in cui � tenuto l'elemento
     * @param elementName il nome dell'elemento che si vuole
     * @return il primo elemento con il nome specificato che c'� nel file
     */
    public String readElementFromFileXml(String fileName , String tagName, String elementName)  {
        File xmlFile = new File(fileName);
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder;
        try {
            builder = factory.newDocumentBuilder();
            Document doc;
            doc = builder.parse(xmlFile);
            return getElement(doc, tagName, elementName);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * funzione che restituisce il primo elemento che si trova nel file con specifico tagName e element Name
     * @param doc una varabile Document che si crea quando si apre un file xml con document builder
     * @param tagName il tag che contiene l'elemento che si desidera
     * @param elementName l'elemento che si desidera
     * @return l'elemento in formato stringa
     */
    //restituisce il primo elemento che si trova nel file xml con le caratteristiche passate in param
    private static String getElement(Document doc, String tagName, String elementName) {
        Node settingNode = doc.getElementsByTagName(tagName).item(0);

        Element settingElement = (Element) settingNode;
        // System.out.println(elementName + " = " + studentId);
        return settingElement.getElementsByTagName(elementName).item(0).getTextContent();
    }
}
