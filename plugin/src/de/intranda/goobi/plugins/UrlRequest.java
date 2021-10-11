package de.intranda.goobi.plugins;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.io.IOUtils;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

import de.intranda.utils.CommonUtils;

public class UrlRequest {
    
    public static String inputEncoding = "utf-8";
    public static String outputEncoding = "iso-8859-15";
    
    public static String getMarcRecordFromOAI(String id) {

        String urlPrefix = "http://aleph.csic.es/OAI?verb=GetRecord&metadataPrefix=marc21&identifier=oai:csic.aleph:MAD01-";
        Document oaiDoc = null;
        try {
            String urlString = urlPrefix + id;
            String oaiRecord = getWebContent(urlString);
            oaiDoc = CommonUtils.getDocumentFromString(oaiRecord, inputEncoding);
        } catch (IOException e) {
//            logger.error("Failed to download oai document");
            return null;
        }
        if (oaiDoc == null) {
//            logger.error("Failed to download oai document");
            return null;
        }
        Element rootElement = oaiDoc.getRootElement();
        Element recordElement = null;
        for (Object obj : rootElement.getChildren()) {
            if (obj instanceof Element) {
                Element ele = (Element) obj;
                if (ele.getName().contentEquals("GetRecord")) {
                    recordElement = ele;
                    break;
                }
            }
        }
        if (recordElement == null) {
//            logger.error("No record element found");
            return null;
        }
        Element marcElement = (Element) recordElement.getChildren().get(0);
        Document marcDoc = new Document((Element) marcElement.clone());
        return getStringFromDocument(marcDoc, outputEncoding);
    }
    
    public static String getStringFromDocument(Document document, String encoding) {
        if (document == null) {
//            logger.warn("Trying to convert null document to String. Aborting");
            return null;
        }
        if(encoding == null)
            encoding = CommonUtils.encoding;
        
        XMLOutputter outputter = new XMLOutputter();
        Format xmlFormat = outputter.getFormat();
        if (!(encoding == null) && !encoding.isEmpty())
            xmlFormat.setEncoding(encoding);
//      xmlFormat.setOmitDeclaration(true);
        xmlFormat.setExpandEmptyElements(true);
        outputter.setFormat(xmlFormat);
        String docString = outputter.outputString(document);

        return docString;
    }
    
    public static String getWebContent(String urlString) throws IOException {
        String answer = "";
        StringWriter writer = null;
        URLConnection yc = null;
        InputStream in = null;
        URL url = new URL(urlString);
        yc = url.openConnection();
        // BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
        writer = new StringWriter();
        in = yc.getInputStream();
        IOUtils.copy(in, writer, inputEncoding);
        answer = writer.toString();

        // String inputLine = null;
        // while ((inputLine = in.readLine()) != null) {
        // answer += (inputLine + "\n");
        // }
        in.close();
        writer.close();
        return answer;
    }
    
    public static void main(String[] args) {
        String id = "000168648";
        System.out.println(getMarcRecordFromOAI(id));
    }

}
