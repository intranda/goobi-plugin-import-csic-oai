package de.intranda.goobi.plugins;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import net.xeoh.plugins.base.annotations.PluginImplementation;

import org.apache.commons.configuration.SubnodeConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.configuration.tree.xpath.XPathExpressionEngine;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.goobi.production.Import.DocstructElement;
import org.goobi.production.Import.ImportObject;
import org.goobi.production.Import.Record;
import org.goobi.production.enums.ImportReturnValue;
import org.goobi.production.enums.ImportType;
import org.goobi.production.enums.PluginType;
import org.goobi.production.plugin.interfaces.IImportPlugin;
import org.goobi.production.plugin.interfaces.IPlugin;
import org.goobi.production.properties.ImportProperty;
import org.jdom.Content;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.Namespace;
import org.jdom.input.SAXBuilder;
import org.jdom.transform.XSLTransformer;

import ugh.dl.DigitalDocument;
import ugh.dl.DocStruct;
import ugh.dl.Fileformat;
import ugh.dl.Metadata;
import ugh.dl.MetadataType;
import ugh.dl.Prefs;
import ugh.exceptions.DocStructHasNoTypeException;
import ugh.exceptions.MetadataTypeNotAllowedException;
import ugh.exceptions.PreferencesException;
import ugh.exceptions.TypeNotAllowedAsChildException;
import ugh.exceptions.TypeNotAllowedForParentException;
import ugh.exceptions.WriteException;
import ugh.fileformats.mets.MetsMods;
import de.intranda.goobi.plugins.utils.ModsUtils;
import de.intranda.utils.CommonUtils;
import de.sub.goobi.Beans.Prozess;
import de.sub.goobi.Beans.Prozesseigenschaft;
import de.sub.goobi.Persistence.ProzessDAO;
import de.sub.goobi.config.ConfigMain;
import de.sub.goobi.config.ConfigPlugins;
import de.sub.goobi.helper.Helper;
import de.sub.goobi.helper.exceptions.DAOException;
import de.sub.goobi.helper.exceptions.SwapException;

@PluginImplementation
public class CSICOAIImport implements IImportPlugin, IPlugin {

    private static final Logger logger = Logger.getLogger(CSICOAIImport.class);

    private static final String NAME = "CSICOAIImport";
    private static final String VERSION = "1.0.20140908";// + CommonUtils.getDateAsVersionNumber();
    private static final String XSLT_PATH = ConfigMain.getParameter("xsltFolder") + "MARC21slim2MODS3.xsl";
    // private static final String XSLT_PATH = "resources/" + "MARC21slim2MODS3.xsl";
    // private static final String MODS_MAPPING_FILE = "resources/" + "mods_map.xml";
    public static final String MODS_MAPPING_FILE = ConfigMain.getParameter("xsltFolder") + "mods_map.xml";
    protected static final String METADATA_LOGICAL_PAGE_NUMBER = "logicalPageNumber";
    protected static final String METADATA_PHYSICAL_PAGE_NUMBER = "physPageNumber";
    private static final Namespace marc_namespace = Namespace.getNamespace("marc", "http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"); 

    private static final String idPrefix = "";
    public final File exportFolder = new File(ConfigPlugins.getPluginConfig(this).getString("importFolder", "/opt/digiverso/ftp-import/"));
    public final String projectIdentifierTitle = (ConfigPlugins.getPluginConfig(this).getString("projectIdentifier", "projectIdentifier"));
    public final String outputEncoding = (ConfigPlugins.getPluginConfig(this).getString("outputEncoding", "iso-8859-15"));
    public final String inputEncoding = (ConfigPlugins.getPluginConfig(this).getString("inputEncoding", "utf-8"));
    public final String conversionEncoding = (ConfigPlugins.getPluginConfig(this).getString("conversionEncoding", "iso-8859-15"));
    public final boolean writeCurrentNoToMultiVolume = (ConfigPlugins.getPluginConfig(this).getBoolean("writeCurrentNoToMultiVolume", false));
    public final boolean writeCurrentNoSortingToMultiVolume = (ConfigPlugins.getPluginConfig(this).getBoolean("writeCurrentNoSortingToMultiVolume",
            false));
    public final boolean useSquareBracketsForVolume = (ConfigPlugins.getPluginConfig(this).getBoolean("useSquareBracketsForVolume", false));
    public final boolean addVolumeNoToTitle = (ConfigPlugins.getPluginConfig(this).getBoolean("addVolumeNoToTitle", false));
    private final boolean deleteOriginalImages = ConfigPlugins.getPluginConfig(this).getBoolean("deleteOriginalImages", true);
    private final boolean deleteTempFiles = ConfigPlugins.getPluginConfig(this).getBoolean("deleteTempFiles", false);
    private final boolean copyImages = ConfigPlugins.getPluginConfig(this).getBoolean("copyImages", true);
    private final boolean updateExistingRecords = ConfigPlugins.getPluginConfig(this).getBoolean("updateExistingRecords", true);
    private final boolean logConversionLoss = ConfigPlugins.getPluginConfig(this).getBoolean("logConversionLoss", false);
    private final boolean relatedSeriesIsAnchor = ConfigPlugins.getPluginConfig(this).getBoolean("relatedSeriesIsAnchor", false);
    private final String urlPrefix = ConfigPlugins.getPluginConfig(this).getString("url_aleph",
            "http://aleph.csic.es/OAI?verb=GetRecord&metadataPrefix=marc21&identifier=oai:csic.aleph:MAD01-");
    
    private String data;

    private Prefs prefs;
    private File importFolder = null; // goobi temp folder

    private Map<String, String> marcStructTypeMap = new HashMap<String, String>();
    private Map<String, String> projectsCollectionsMap = new HashMap<String, String>();
    private Map<String, File> recordImageMap = new HashMap<String, File>();
    private HashMap<String, Boolean> idMap = new HashMap<String, Boolean>(); // maps to true all records with reoccuring ids (PPNs)

    private String identifierSuffix;
    private String currentIdentifier;
    private String currentTitle;
    private String currentAuthor;
    private List<String> currentCollectionList;
    private File currentImageFolder;

    // private boolean deleteTempFiles = false;
    // private boolean copyImages = true;
    // private boolean deleteOriginalImages = true;
    // private boolean updateExistingRecords = true;

    @SuppressWarnings("unchecked")
    public CSICOAIImport() {

        String catalogueName = "MAD01";
        try {
            catalogueName = urlPrefix.substring(urlPrefix.lastIndexOf(":"));
            catalogueName = catalogueName.replaceAll("\\W", "");
        } catch (IndexOutOfBoundsException e) {
            logger.error(e);
        }
        List<String> docTypesMarc = new ArrayList<String>();
        List<String> docTypesGoobi = new ArrayList<String>();
        try {
            XMLConfiguration config = ConfigPlugins.getPluginConfig(this);
            config.setExpressionEngine(new XPathExpressionEngine());
            SubnodeConfiguration docTypeConfig = config.configurationAt("DocTypeConfig[@catalogue=\"" + catalogueName + "\"]");
            docTypesMarc = docTypeConfig.getList("DocType/@typeOfResource");
            docTypesGoobi = docTypeConfig.getList("DocType/@goobiName");
        } catch (IllegalArgumentException e) {
            logger.error(e);
            docTypesMarc.add("monographic");
            docTypesGoobi.add("Monograph");
        }
        for (int i = 0; i < docTypesMarc.size(); i++) {
            String marcName = docTypesMarc.get(i);
            String goobiName = docTypesGoobi.get(i);
            marcStructTypeMap.put("?" + marcName, goobiName);
        }

        List<String> projectList = ConfigPlugins.getPluginConfig(this).getList("project[@name]");
        List<String> collectionList = ConfigPlugins.getPluginConfig(this).getList("project[@collection]");

        for (int i = 0; i < projectList.size(); i++) {
            String projectName = projectList.get(i);
            String collectionName = collectionList.get(i);
            projectsCollectionsMap.put(projectName, collectionName);
        }

    }

    @Override
    public List<ImportObject> generateFiles(List<Record> records) {
        ArrayList<ImportObject> ret = new ArrayList<ImportObject>();

        for (Record r : records) {
            logger.info("Processing " + r.getId());
            // Data conversion
            data = r.getData();
            File imageDir = recordImageMap.get(r.getId());
            currentImageFolder = imageDir;
            currentIdentifier = r.getId();
            identifierSuffix = null;

            File projectDir = imageDir.getParentFile();
            if (projectDir.getName().toLowerCase().contains("tiff") || projectDir.getName().toLowerCase().contains("jpeg")
                    || projectDir.getName().toLowerCase().contains("difusion")) {
                projectDir = projectDir.getParentFile();
            }

            // currentCollectionList = r.getCollections();
            currentCollectionList = new ArrayList<String>();
            String collection = projectsCollectionsMap.get(projectDir.getName());
            if (collection != null) {
                currentCollectionList.add(collection);
            }
            Fileformat ff = convertData();
            addPages(ff, imageDir);
            ImportObject io = new ImportObject();

            if (ff != null) {

                // add collection as metadata
                addProject(ff, projectDir.getName());

                // writing converted data into Import("temp") folder
                try {
                    MetsMods mm = new MetsMods(prefs);
                    mm.setDigitalDocument(ff.getDigitalDocument());
                    String fileName = getImportFolder() + getProcessTitle();
                    logger.debug("Writing '" + fileName + "'...");
                    mm.write(fileName);

                    List<? extends Metadata> mdList =
                            mm.getDigitalDocument().getLogicalDocStruct().getAllMetadataByType(prefs.getMetadataTypeByName("TitleDocMain"));

                    logger.debug("copying image files from " + imageDir.getAbsolutePath() + "...");
                    copyImageFiles(imageDir);
                    io.setProcessTitle(getProcessTitle().replace(".xml", ""));
                    io.setMetsFilename(fileName);
                    io.setImportReturnValue(ImportReturnValue.ExportFinished);

                    Prozesseigenschaft property = new Prozesseigenschaft();

                    property.setTitel(projectIdentifierTitle);
                    property.setWert(projectDir.getName());
                    ArrayList<Prozesseigenschaft> propertyList = new ArrayList<Prozesseigenschaft>();
                    propertyList.add(property);
                    io.setProcessProperties(propertyList);

                } catch (PreferencesException e) {
                    logger.error(e.getMessage(), e);
                    io.setImportReturnValue(ImportReturnValue.InvalidData);
                    io.setErrorMessage(e.getMessage());
                } catch (WriteException e) {
                    logger.error(e.getMessage(), e);
                    io.setImportReturnValue(ImportReturnValue.WriteError);
                    io.setErrorMessage(e.getMessage());
                }
            } else {
                io.setImportReturnValue(ImportReturnValue.InvalidData);
                io.setErrorMessage("FileFormat is null");
            }
            ret.add(io);
        }
        return ret;
    }

    private void addPages(Fileformat ff, File imageDir) {

        try {
            DocStruct topLogStruct = ff.getDigitalDocument().getLogicalDocStruct();
            if (topLogStruct.getType().isAnchor()) {
                topLogStruct = topLogStruct.getAllChildren().get(0);
            }

            String[] imageFileArray = imageDir.list(CommonUtils.ImageFilter);
            if (imageFileArray == null || imageFileArray.length == 0) {
                File[] subDirs = imageDir.listFiles(CommonUtils.DirFilter);
                if (subDirs != null && subDirs.length > 0) {
                    for (File dir : subDirs) {
                        String[] imageList = dir.list(CommonUtils.ImageFilter);
                        if (imageList != null && imageList.length > 0) {
                            imageFileArray = imageList;
                            break;
                        }
                    }
                }

                if (imageFileArray == null || imageFileArray.length == 0) {
                    logger.error("Found no images to identify with this process");
                    return;
                }

            }
            Arrays.sort(imageFileArray);

            int count = 1;
            for (String filename : imageFileArray) {
                DocStruct page = ff.getDigitalDocument().createDocStruct(prefs.getDocStrctTypeByName("page"));

                MetadataType logpageType = prefs.getMetadataTypeByName(METADATA_LOGICAL_PAGE_NUMBER);
                MetadataType physpageType = prefs.getMetadataTypeByName(METADATA_PHYSICAL_PAGE_NUMBER);
                Metadata mdLogPage = new Metadata(logpageType);
                Metadata mdPhysPage = new Metadata(physpageType);
                mdLogPage.setValue("uncounted");
                mdPhysPage.setValue("" + count);
                count++;

                page.addMetadata(mdLogPage);
                page.addMetadata(mdPhysPage);
                topLogStruct.addReferenceTo(page, "logical_physical");
                ff.getDigitalDocument().getPhysicalDocStruct().addChild(page);

            }

        } catch (TypeNotAllowedForParentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (PreferencesException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (TypeNotAllowedAsChildException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (MetadataTypeNotAllowedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Uses image directories to retrieve metadata via oai
     * 
     */
    @Override
    public List<Record> generateRecordsFromFilenames(List<String> filenames) {

        List<Record> recordList = new ArrayList<Record>();
        List<Record> importList = new ArrayList<Record>();
        HashMap<Record, File> updateRecordMap = new HashMap<Record, File>();
        for (String dirString : filenames) {

            String[] parts = dirString.split("::");
            if (parts == null || parts.length != 2) {
                // return recordList;
                continue;
            }
            String projectName = parts[0].trim();
            String dirName = parts[1].trim();
            File projectDir = new File(exportFolder, projectName);
            File recordDir = new File(projectDir, dirName);

            String[] idStrings = parseFolderName(dirName);
            String idString = idStrings[0];

            idString = idPrefix + idString;
            logger.debug("Found record id " + idString);
            String marcRecord = getMarcRecordFromOAI(idString);
            if (marcRecord == null) {
                logger.error("Unable to retrieve marc-record " + idString + ".");
                continue;
            }

            String idNumber = idString.replaceAll("\\D", "");
            if (idMap.get(idNumber) == null) {
                idMap.put(idNumber, false);
            } else {
                idMap.put(idNumber, true);
            }

            Record record = new Record();
            record.setId(recordDir.getAbsolutePath());
            record.setData(marcRecord);
            recordList.add(record);
        }

        for (Record record : recordList) {

            File imageFolder = new File(record.getId());
            String folderName = imageFolder.getName();
            String[] idParts = parseFolderName(folderName);
            String idNumber = idParts[0].replaceAll("\\D", "");

            if (idMap.get(idNumber) != null && idMap.get(idNumber) == true) {
                if (idParts[2] != null && !idParts[2].trim().isEmpty() && !idParts[2].trim().replaceAll("_", "").contentEquals("V00")) {
                    record.setId(idParts[0] + "_" + idParts[2]);
                } else if (idParts[1] != null) {
                    record.setId(idParts[0] + "_" + idParts[1]);
                } else {
                    record.setId(idParts[0]);
                }
            } else {
                record.setId(idParts[0]);
            }

            File oldFile = searchForExistingData(record);
            recordImageMap.put(record.getId(), imageFolder);
            if (oldFile != null) {
                if (updateExistingRecords) {
                    logger.info("Found existing record. Updating.");
                    updateRecordMap.put(record, oldFile);
                } else {
                    Helper.setFehlerMeldung("Cannot import record " + record.getId() + " : A process with this title already exists.");
                }
            } else {
                importList.add(record);
            }
        }

        for (Record record : updateRecordMap.keySet()) {
            if (!updateOldRecord(record, updateRecordMap.get(record))) {
                Helper.setFehlerMeldung("Error updating record " + record.getId());
            } else {
                Helper.setMeldung("Updated process " + record.getId());
            }
        }

        return importList;
    }

    /**
     * 
     * creates a backup of the oldMetaFile and replaces the metafile with the Record record
     * 
     * @param record
     * @param oldMetaFile
     */
    private boolean updateOldRecord(Record record, File oldMetaFile) {

        File imageDir = recordImageMap.get(record.getId());

        currentIdentifier = record.getId();
        currentImageFolder = imageDir;
        data = record.getData();

        File projectDir = imageDir.getParentFile();
        if (projectDir.getName().toLowerCase().contains("tiff") || projectDir.getName().toLowerCase().contains("jpeg")
                || projectDir.getName().toLowerCase().contains("difusion")) {
            projectDir = projectDir.getParentFile();
        }

        currentCollectionList = new ArrayList<String>();
        String collection = projectsCollectionsMap.get(projectDir.getName());
        if (collection != null) {
            currentCollectionList.add(collection);
        }
        Fileformat ff = convertData();
        addProject(ff, projectDir.getName());

        File[] imageList = imageDir.listFiles(CommonUtils.ImageFilter);
        if (imageList == null || imageList.length == 0) {
            File goobiImageDir = new File(oldMetaFile.getParent(), "images");
            addPages(ff, goobiImageDir);
        } else {
            addPages(ff, imageDir);
        }

        logger.info("Replacing old matadata in metadata folder " + oldMetaFile.getParent() + " with new data");

        // renaming old metadata files to keep as backup
        String newMetaFileName = oldMetaFile.getAbsolutePath();
        File oldAnchorFile = new File(oldMetaFile.getParent(), "meta_anchor.xml");
        if (oldAnchorFile.isFile()) {
            oldAnchorFile.renameTo(new File(oldMetaFile.getParent(), oldAnchorFile.getName().replace(".xml", ".preIntrandaImport.xml")));
        }
        oldMetaFile.renameTo(new File(oldMetaFile.getParent(), oldMetaFile.getName().replace(".xml", ".preIntrandaImport.xml")));
        try {
            if (ff == null) {
                logger.error("Mets document is null. Aborting import");
                return false;
            }
            String fileName = newMetaFileName;
            logger.debug("Writing '" + fileName + "' into existing folder...");
            ff.write(fileName);
            if (imageList != null && imageList.length > 0) {
                copyImageFiles(imageDir);
            }
            // getting anchor file
            if (!importFolder.isDirectory()) {
                logger.warn("no hotfolder found. Cannot get anchor files");
            } else {

                // create source ("import") Folder
                File sourceFolder = null;
                if (importFolder != null) {
                    File tempDir = new File(importFolder, getProcessTitle().replace(".xml", ""));
                    sourceFolder = new File(tempDir, "import");
                    sourceFolder.mkdirs();
                }

                // updating import folders
                if (sourceFolder != null && sourceFolder.isDirectory()) {
                    File tempDir = sourceFolder.getParentFile();
                    File metadataDir = oldMetaFile.getParentFile();

                    CommonUtils.moveDir(tempDir, metadataDir, true);
                }

                // purging old temp files
                for (File file : importFolder.listFiles(CommonUtils.XmlFilter)) {
                    if (file.getName().contains(record.getId()))
                        file.delete();
                }
            }

            // ret.put(getProcessTitle(), ImportReturnValue.ExportFinished);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return false;
        } catch (WriteException e) {
            logger.error(e.getMessage(), e);
            return false;
        } catch (PreferencesException e) {
            logger.error(e.getMessage(), e);
            return false;
        }
        return true;
    }

    private String getMarcRecordFromOAI(String id) {

        Document oaiDoc = null;
        try {
            String urlString = urlPrefix + id;
            String oaiRecord = getWebContent(urlString);
            logger.trace("url = " + urlString);
            oaiDoc = CommonUtils.getDocumentFromString(oaiRecord, inputEncoding);
        } catch (IOException e) {
            logger.error("Failed to download oai document");
            return null;
        }
        if (oaiDoc == null) {
            logger.error("Failed to download oai document");
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
            logger.error("No record element found");
            return null;
        }
        Element marcElement = (Element) recordElement.getChildren().get(0);
        Document marcDoc = new Document((Element) marcElement.clone());
        return CommonUtils.getStringFromDocument(marcDoc, outputEncoding);
    }

    @Override
    public void setPrefs(Prefs prefs) {
        this.prefs = prefs;

    }

    @Override
    public List<ImportType> getImportTypes() {
        List<ImportType> answer = new ArrayList<ImportType>();
        // answer.add(ImportType.FILE);
        answer.add(ImportType.FOLDER);

        return answer;
    }

    @Override
    public PluginType getType() {
        return PluginType.Import;
    }

    @Override
    public String getTitle() {
        return getDescription();
    }

    @Override
    public String getDescription() {
        return NAME + " v" + VERSION;
    }

    @Override
    public void setData(Record r) {
        data = r.getData();
    }

    @Override
    /**
     * 
     * Specialized convertData to convert only the specified String inString from marc to mods
     * 
     * @param inString
     * @return
     */
    public Fileformat convertData() {
        Fileformat ff = null;
        Document doc;
        StringReader sr = null;
        try {
            sr = new StringReader(data);
            ByteArrayInputStream bais = new ByteArrayInputStream(data.getBytes(conversionEncoding));
            doc = new SAXBuilder().build(bais);
            // remove namespaces
            Element docRoot = doc.getRootElement();
            docRoot = getActualRecordElement(docRoot);
            docRoot = setNamespaceRecursive(docRoot, null);
            Element newRecord = new Element("record");
            List<Element> eleList = new ArrayList<Element>();
            for (Object obj : docRoot.getChildren()) {
                Element child = (Element) obj;
                eleList.add(child);
            }
            for (Element element : eleList) {
                element.detach();
            }
            newRecord.setContent(eleList);
            for (Object obj : newRecord.getChildren()) {
                Element child = (Element) obj;
                child.setNamespace(null);
            }
            // newRecord = removeDuplicateChildren(newRecord);
            docRoot.detach();
            doc.setRootElement(newRecord);

            // logger.debug(new XMLOutputter().outputString(doc));
            if (doc != null && doc.hasRootElement()) {
                XSLTransformer transformer = new XSLTransformer(XSLT_PATH);
                Document docMods = transformer.transform(doc);
                // logger.debug(new XMLOutputter().outputString(docMods));
                ff = new MetsMods(prefs);
                DigitalDocument dd = new DigitalDocument();
                ff.setDigitalDocument(dd);

                Element eleMods = docMods.getRootElement();
                if (eleMods.getName().equals("modsCollection")) {
                    eleMods = eleMods.getChild("mods", null);
                }

                // get volume infos
                String[] volumeInfos = parseFolderName(currentImageFolder.getName());
                int currentVolume = 0;
                try {
                    currentVolume = Integer.valueOf(volumeInfos[2]);
                } catch (NumberFormatException e) {
                    currentVolume = 0;
                }

                // Determine the root docstruct type
                String dsType = null;
                String dsAnchorType = null;
                boolean belongsToPeriodical = false;
                boolean belongsToSeries = false;
                boolean isManuscript = false;
                boolean belongsToMultiVolume = false;

                // handle TypeOfResource
                List<Element> eleTypeOfResourceList = eleMods.getChildren("typeOfResource", null);
                if (eleTypeOfResourceList != null) {
                    for (Element eleTypeOfResource : eleTypeOfResourceList) {
                        String resourceLabel = eleTypeOfResource.getAttributeValue("displayLabel");
                        if (resourceLabel != null && resourceLabel.contains("SE")) {
                            belongsToPeriodical = true;
                        }
                        if ("yes".equals(eleTypeOfResource.getAttributeValue("manuscript"))) {
                            isManuscript = true;
                        }
                        if (marcStructTypeMap.get("?" + eleTypeOfResource.getTextTrim()) != null) {
                            dsType = marcStructTypeMap.get("?" + eleTypeOfResource.getTextTrim());
                        } else {
                            dsType = "Monograph";
                        }
                    }
                }

                // handle physicalDescription
                List<Element> physicalDescriptionList = eleMods.getChildren("physicalDescription", null);
                if (physicalDescriptionList != null) {
                    for (Element physDescr : physicalDescriptionList) {
                        List<Element> eleFormList = physDescr.getChildren("form", null);
                        if (eleFormList != null) {
                            for (Element eleForm : eleFormList) {
                                if (eleForm.getAttribute("authority") != null && eleForm.getValue().contentEquals("Manuscrito")) {
                                    isManuscript = true;
                                }
                            }
                        }
                    }
                }

                // handle relatedSeries
                List<Element> eleRelatedSeriesList = eleMods.getChildren("relatedItem", null);
                if (relatedSeriesIsAnchor && eleRelatedSeriesList != null) {
                    for (Element eleRelatedSeries : eleRelatedSeriesList) {

                        if (eleRelatedSeries != null && eleRelatedSeries.getAttribute("type") != null
                                && eleRelatedSeries.getAttribute("type").getValue().contentEquals("series")) {
                            belongsToSeries = true;
                        }
                    }
                }

                if (idMap.get(getStrippedId(currentIdentifier)) != null) {

                    if (idMap.get(getStrippedId(currentIdentifier)) == true) {
                        belongsToMultiVolume = true;
                    } else if ((identifierSuffix != null && identifierSuffix.startsWith("V")) && ((!belongsToPeriodical && !belongsToSeries))) {
                        belongsToMultiVolume = true;
                    }
                    // This volume is part of a Series/Multivolume work
                    // if (!belongsToPeriodical && !belongsToSeries) {
                    // }
                }

                //manuscript is set in config, so we ignore the setting here
                isManuscript = false;

                if (belongsToPeriodical) {
                    dsAnchorType = "Periodical";
                    if (dsType == "Monograph" || dsType == null) {
                        dsType = "PeriodicalVolume";
                    }
                } else if (belongsToSeries) {
                    dsAnchorType = "Series";
                    if (isManuscript) {
                        dsType = "Manuscript";
                    } else if (dsType == "Monograph" || dsType == null) {
                        dsType = "Monograph";
                    }
                } else if (isManuscript) {
                    dsType = "Manuscript";
                }
                // Multivolume may be part of a Series or Periodical. In that case, attach the volumes to the Series/Periodical
                if (belongsToMultiVolume) {
                    if (!belongsToPeriodical && !belongsToSeries) {
                        dsAnchorType = "MultiVolumeWork";
                    }
                    if (isManuscript) {
                        dsType = "Manuscript";
                    } else if (!belongsToPeriodical && (dsType == "Monograph" || dsType == null)) {
                        dsType = "Volume";
                    }
                }

                // remove unnecessary suffixes for everything but multivolumes
                if (!belongsToMultiVolume) {
                    if (idMap.get(getStrippedId(currentIdentifier)) != null && idMap.get(getStrippedId(currentIdentifier)) == true) {
                        // need suffix
                    } else {
                        identifierSuffix = null;
                    }
                }

                logger.debug("Docstruct type: " + dsType);
                DocStruct dsVolume = dd.createDocStruct(prefs.getDocStrctTypeByName(dsType));
                if (dsVolume == null) {
                    logger.error("Could not create DocStructType " + dsVolume);
                    return null;
                }
                DocStruct dsAnchor = null;
                if (dsAnchorType != null) {
                    dsAnchor = dd.createDocStruct(prefs.getDocStrctTypeByName(dsAnchorType));
                    if (dsAnchor == null) {
                        logger.error("Could not create DocStructType " + dsAnchorType);
                    }
                    try {
                        dsAnchor.addChild(dsVolume);
                    } catch (TypeNotAllowedAsChildException e) {
                        logger.error("Could not atach " + dsAnchorType + " to anchor " + dsType);
                    }
                    dd.setLogicalDocStruct(dsAnchor);
                } else {
                    dd.setLogicalDocStruct(dsVolume);
                }

                //get volume number of this item
                Integer volumeNo = ModsUtils.getNumberFromString(this.getCurrentSuffix());
                if (volumeNo == null) {
                    volumeNo = currentVolume;
                }

                DocStruct dsBoundBook = dd.createDocStruct(prefs.getDocStrctTypeByName("BoundBook"));
                dd.setPhysicalDocStruct(dsBoundBook);
                // Collect MODS metadata
                ModsUtils.parseModsSection(this, dsVolume, dsAnchor, dsBoundBook, eleMods, currentVolume, volumeInfos[1]);
                // currentIdentifier = ModsUtils.getIdentifier(prefs, dsVolume);
                currentTitle = ModsUtils.getTitle(prefs, dsVolume);
                currentAuthor = ModsUtils.getAuthor(prefs, dsVolume);
                logger.debug("Author:" + currentAuthor + ", Title: " + currentTitle);

                // create source ("import") Folder
                File sourceFolder = null;
                if (importFolder != null) {
                    File tempDir = new File(importFolder, getProcessTitle().replace(".xml", ""));
                    sourceFolder = new File(tempDir, "import");
                    sourceFolder.mkdirs();
                }

                // Check if we are part of a series, and if so, create logical DocStruct accordingly
                try {
                    List<? extends Metadata> seriesIDList = null;
                    if (dsAnchor != null) {
                        seriesIDList = dsAnchor.getAllMetadataByType(prefs.getMetadataTypeByName("CatalogIDDigital"));
                    }
                    // for (Metadata metadata : seriesIDList) {
                    // }
                    if (seriesIDList != null && !seriesIDList.isEmpty()) {
                        logger.debug("Record is part of a series");
                        // isSeriesVolume = true;
                        dsAnchor.addChild(dsVolume);
                        dd.setLogicalDocStruct(dsAnchor);
                        String volumeName = dsVolume.getType().getName();
                        if (volumeName.contentEquals("SingleManuscript")) {
                            dsVolume.setType(prefs.getDocStrctTypeByName("Manuscript"));
                        }
                    } else {
                        dd.setLogicalDocStruct(dsVolume);
                        logger.debug("Record is not part of a series");
                        String volumeName = dsVolume.getType().getName();
                        if (volumeName.contentEquals("SerialVolume")) {
                            dsVolume.setType(prefs.getDocStrctTypeByName("Monograph"));
                        }
                    }
                } catch (TypeNotAllowedAsChildException e) {
                    logger.error("Child Type not allowed", e);
                }

                // log conversion-errors from marc to mods
                if (logConversionLoss) {
                    File tempMods = new File("/opt/digiverso/logs/CSIC", "tempMods.xml");
                    Document lossDoc = getMarcModsLoss(doc, dd.toString());
                    File lossFile = new File("/opt/digiverso/logs/CSIC", currentIdentifier + identifierSuffix + "_MarcLoss.xml");
                    CommonUtils.getFileFromDocument(lossFile, lossDoc);
                }

                if (!deleteTempFiles) {
                    try {
                        File modsFile = new File(sourceFolder, "modsTemp.xml");
                        File marcFile = new File(sourceFolder, "marcRecord.xml");
                        CommonUtils.getFileFromDocument(modsFile, docMods);
                        CommonUtils.writeTextFile(data, marcFile, conversionEncoding, false);
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }

                // Add 'pathimagefiles'
                try {
                    Metadata mdForPath = new Metadata(prefs.getMetadataTypeByName("pathimagefiles"));
                    mdForPath.setValue("./" + currentIdentifier);
                    dsBoundBook.addMetadata(mdForPath);
                } catch (MetadataTypeNotAllowedException e1) {
                    logger.error("MetadataTypeNotAllowedException while reading images", e1);
                } catch (DocStructHasNoTypeException e1) {
                    logger.error("DocStructHasNoTypeException while reading images", e1);
                }

                // Add collection names attached to the current record
                if (currentCollectionList != null) {
                    MetadataType mdTypeCollection = prefs.getMetadataTypeByName("singleDigCollection");
                    for (String collection : currentCollectionList) {
                        Metadata mdCollection = new Metadata(mdTypeCollection);
                        mdCollection.setValue(collection);
                        dsVolume.addMetadata(mdCollection);
                        //                        if (dsAnchor != null) {
                        //                            dsAnchor.addMetadata(mdCollection);
                        //                        }
                    }
                }
            }

        } catch (JDOMException e) {
            logger.error(e.getMessage(), e);
            return null;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } catch (PreferencesException e) {
            logger.error(e.getMessage(), e);
        } catch (TypeNotAllowedForParentException e) {
            logger.error(e.getMessage(), e);
        } catch (MetadataTypeNotAllowedException e) {
            logger.error(e.getMessage(), e);
        } finally {
            // try{
            if (sr != null) {
                sr.close();
            }
            // } catch(IOException e) {
            // logger.error("Error closing String reader");
            // }
        }

        return ff;
    }

    private Element getActualRecordElement(Element root) {
        if(root.getName().equals("record") && root.getNamespace().getPrefix().equals("marc")) {
            return root;
        } else {
            Iterator<Content> iterator = root.getDescendants();
            while(iterator.hasNext()) {
                Content content = iterator.next();
                if(content instanceof Element && ((Element) content).getName().equals("record") && ((Element) content).getNamespace().getPrefix().equals("marc")) {
                    return (Element) content;
                }
            }
        }
        return root;
    }

    private String getStrippedId(String id) {
        String[] parts = id.split("_");
        if (parts == null || parts.length < 2) {
            return id.replaceAll("\\D", "");
        } else {
            return parts[0].replaceAll("\\D", "");
        }
    }

    @Override
    public String getImportFolder() {
        return importFolder.getAbsolutePath() + File.separator;
    }

    @Override
    public String getProcessTitle() {
        String title = "";
        if (StringUtils.isNotBlank(currentTitle)) {
            String atstsl = createAtstsl(currentTitle, currentAuthor);
            if (atstsl != null) {
                title = createAtstsl(currentTitle, currentAuthor).toLowerCase() + "_" + currentIdentifier + ".xml";
            } else {
                title = currentIdentifier + ".xml";
            }
        } else {
            title = currentIdentifier + ".xml";
        }
        return title;
    }

    @Override
    public void setImportFolder(String folder) {
        importFolder = new File(folder);

    }

    @Override
    public List<Record> splitRecords(String records) {
        return null;
    }

    @Override
    public List<Record> generateRecordsFromFile() {
        return null;
    }

    @Override
    public void setFile(File importFile) {
    }

    @Override
    public List<String> splitIds(String ids) {
        return null;
    }

    @Override
    public List<ImportProperty> getProperties() {
        return null;
    }

    @Override
    public List<String> getAllFilenames() {
        ArrayList<String> projectList = new ArrayList<String>(projectsCollectionsMap.keySet());
        ArrayList<String> filenameList = new ArrayList<String>();
        try {
            for (String project : projectList) {
                File projectDir = new File(exportFolder, project);
                if (projectDir.isDirectory()) {
                    File imageDir = projectDir;
                    File[] subDirs = projectDir.listFiles(CommonUtils.DirFilter);
                    for (File dir : subDirs) {
                        if (dir.getName().toLowerCase().contains("tiff")) {
                            imageDir = dir;
                            break;
                        }
                    }
                    // filenameList.add(xmlDir.getAbsolutePath());
                    File[] dirList = imageDir.listFiles(CommonUtils.DirFilter);
                    if (dirList != null && dirList.length > 0) {
                        for (File recordDir : dirList) {
                            if (recordDir.getName().startsWith("M_")) {
                                // if(updateExistingRecords) {
                                filenameList.add(project + "::\t" + recordDir.getName());
                                // } else {
                                // String identifier = recordDir.getName().split("_")[0];
                                // if(identifier.contentEquals("M")) {
                                // identifier = recordDir.getName().split("_")[1];
                                // }
                                // if(searchForExistingData(identifier) == null) {
                                // filenameList.add(project + "::\t" + recordDir.getName());
                                // } else {
                                // System.out.println("Found record " + recordDir.getName());
                                // }
                                // }
                            }

                        }
                    }
                }
            }
        } catch (Exception e) {
            filenameList.add("ERROR: " + e.getMessage());
            return filenameList;
        } catch (Error e) {
            filenameList.add("ERROR: " + e.getMessage());
            return filenameList;
        }
        Collections.sort(filenameList);
        return filenameList;
    }

    @Override
    public void deleteFiles(List<String> selectedFilenames) {
    }

    @Override
    public List<DocstructElement> getCurrentDocStructs() {
        return null;
    }

    @Override
    public String deleteDocstruct() {
        return null;
    }

    @Override
    public String addDocstruct() {
        return null;
    }

    @Override
    public List<String> getPossibleDocstructs() {
        return null;
    }

    @Override
    public DocstructElement getDocstruct() {
        return null;
    }

    @Override
    public void setDocstruct(DocstructElement dse) {
    }

    private void addProject(Fileformat ff, String projectName) {
        try {
            DocStruct topStruct = ff.getDigitalDocument().getLogicalDocStruct();
            if (topStruct.getAllMetadataByType(prefs.getMetadataTypeByName("ProjectIdentifier")).isEmpty()) {
                Metadata projectInfo = new Metadata(prefs.getMetadataTypeByName("ProjectIdentifier"));
                projectInfo.setValue(projectName);
                topStruct.addMetadata(projectInfo);

                // set also for first subSrtuct if topStruct was an anchor
                if (topStruct.getType().isAnchor()) {
                    DocStruct subStruct = topStruct.getAllChildren().get(0);
                    projectInfo = new Metadata(prefs.getMetadataTypeByName("ProjectIdentifier"));
                    projectInfo.setValue(projectName);
                    subStruct.addMetadata(projectInfo);
                }
            }

        } catch (PreferencesException e) {
            logger.error("Unable to add collection as metadata: " + e.getMessage(), e);
        } catch (MetadataTypeNotAllowedException e) {
            logger.error("Unable to add collection as metadata: " + e.getMessage(), e);
        } catch (DocStructHasNoTypeException e) {
            logger.error("Unable to add collection as metadata: " + e.getMessage(), e);
        } finally {
        }

    }

    /**
     * 
     * Copy the files in exportFolder corresponding to the current record into the importFolder
     * 
     * @param exportFolder
     */
    private void copyImageFiles(File imageDir) {

        if (!copyImages || imageDir == null) {
            logger.debug("No images to copy");
            return;
        }

        File[] imageFiles = imageDir.listFiles(CommonUtils.ImageFilter);
        if (imageFiles == null) {
            logger.debug("No images to copy");
            return;
        }

        // get temp dir
        File tempDir = new File(importFolder, getProcessTitle().replace(".xml", ""));
        File tempImageDir = new File(tempDir, "images");
        File tempTiffDir = new File(tempImageDir, getProcessTitle().replace(".xml", "") + "_tif");
        // File tempOrigDir = new File(tempImageDir, "orig_" + getProcessTitle().replace(".xml", "") + "_tif");
        tempTiffDir.mkdirs();

        // parse all image Files and write them into new Files in the import
        // directory
        List<File> images = Arrays.asList(imageFiles);
        try {
            for (File imageFile : images) {
                String filename = imageFile.getName();
                if (!deleteOriginalImages) {
                    copyFile(imageFile, new File(tempTiffDir, filename));
                    logger.debug("Copying image " + filename);
                } else {
                    CommonUtils.moveFile(imageFile, new File(tempTiffDir, filename), true);
                    // if (!imageFile.renameTo(new File(tempTiffDir, filename))) {
                    // copyFile(imageFile, new File(tempTiffDir, filename));
                    // imageFile.delete();
                    // }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void copyFile(File source, File dest) throws IOException {
        InputStream inStream = new FileInputStream(source);
        BufferedInputStream bis = new BufferedInputStream(inStream);
        FileOutputStream fos = new FileOutputStream(dest);
        byte[] bytes = new byte[8192];
        int count = bis.read(bytes);
        while (count != -1 && count <= 8192) {
            fos.write(bytes, 0, count);
            count = bis.read(bytes);
        }
        if (count != -1) {
            fos.write(bytes, 0, count);
        }
        fos.close();
        bis.close();
    }

    /**
     * Sets the namespace of all Elements within Element root to Namespace ns
     * 
     * @param root
     * @param ns
     * @return
     */
    @SuppressWarnings("unchecked")
    public static Element setNamespaceRecursive(Element root, Namespace ns) {
        List<Element> current = new ArrayList<Element>();
        current.add(root);
        do {
            List<Element> children = new ArrayList<Element>();
            for (Element element : current) {
                element.setNamespace(ns);
                children.addAll(element.getChildren());
            }
            current = children;
        } while (!current.isEmpty());

        return root;
    }

    public String getWebContent(String urlString) throws IOException {
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

    private String[] parseFolderName(String name) {

        String[] values = new String[] { null, null, "V00" };

        if (name.startsWith("M_")) {
            name = name.substring(2);
        }

        String[] parts = name.split("_");
        values[0] = parts[0];
        currentIdentifier = values[0];
        int last = parts.length - 1;
        if (parts.length > 2) {
            //		    values[1] = parts[1];
            //		    int suffixStartIndex = name.lastIndexOf(parts[1])+parts[1].length()+1;
            //		    if(suffixStartIndex < name.length()-1) {		        
            //		        values[2] = name.substring(suffixStartIndex);
            //		    }
            //
            int lastWOVolumeNo = last;
            if (parts[last].startsWith("V")) {
                lastWOVolumeNo = last - 1;
                values[2] = parts[last];
            }
            values[1] = "";
            for (int i = 1; i <= lastWOVolumeNo; i++) {
                values[1] = values[1] + "_" + parts[i];
            }
            values[1] = values[1].substring(1);
            // values[0] = values[0].substring(0, values[0].length() - 1);
            // values[1] = parts[last].replace("V", "");
            // return parts[last-1];
        } else if (parts[last].startsWith("V")) {
            values[2] = parts[last];
        } else if (values[1] != null && !values[1].equals(currentIdentifier)) {
            values[1] = parts[last];
        }
        if (values[2] != null && !values[2].isEmpty() && !values[2].contentEquals("V00")) {
            identifierSuffix = values[2];
        } else if (values[1] != null && !values[1].equals(currentIdentifier)) {
            identifierSuffix = values[1];
        }

        if (identifierSuffix != null) {
            if (identifierSuffix.startsWith("_")) {
                identifierSuffix = identifierSuffix.substring(1);
            }

            currentIdentifier = (currentIdentifier + "_" + identifierSuffix).replaceAll("__", "_");
        }
        return values;

    }

    /**
     * returns the metadatafile meta.xml if a prozess of this name was found, null otherwise
     * 
     * @param processTitle
     * @return
     */
    private File searchForExistingData(Record r) {
        String processTitle = r.getId();

        // For imports with wrong processTitle, correct it
        //		processTitle = processTitle.replace("000471130", "001100392");
        processTitle = processTitle.replace("001363255", "000884278");
        processTitle = processTitle.replace("00045898", "000045898");
        processTitle = processTitle.replace("0000045898", "000045898");
        //

        int index = processTitle.indexOf("_");
        String processId = processTitle;
        String processIdSuffix = "";
        String processIdVolume = "";
        if (index > 0 && index < processTitle.length()) {
            processId = processTitle.substring(0, index);
            processIdSuffix = processTitle.substring(index + 1);
            int vIndex = processIdSuffix.indexOf("V");
            if (vIndex > -1 && vIndex < processIdSuffix.length() - 2) {
                processIdVolume = processIdSuffix.substring(vIndex);
                if (processIdVolume.contains("_")) {
                    processIdVolume = processIdVolume.substring(0, processIdVolume.indexOf("_"));
                }
            }
        }
        String metsFilePath, processDataDirectory;
        ProzessDAO dao = new ProzessDAO();

        try {
            List<Prozess> processList = dao.search("from Prozess where titel LIKE '%" + processId + "%'");

            if (processList == null || processList.isEmpty()) {
                String id = processTitle.split("_")[0] + "_V00";
                processList = dao.search("from Prozess where titel LIKE '%" + id + "'");
            }

            if (processList != null && !processList.isEmpty()) {
                Prozess p = processList.get(0);
                if (processList.size() > 1) {
                    for (Prozess process : processList) {
                        if (process.getTitel().contains(processIdSuffix)) {
                            p = process;
                            break;
                        } else if (p.getTitel().contains(processIdVolume)) {
                            p = process;
                        }
                    }

                }

                p.setTitel(p.getTitel().split("_")[0] + "_" + processTitle);
                r.setId(p.getTitel());
                logger.info("Found existing process '" + p.getTitel() + "'...");
                metsFilePath = p.getMetadataFilePath();
                processDataDirectory = p.getProcessDataDirectory();
                logger.debug("METS file path: " + metsFilePath);
                logger.debug("Process data path: " + processDataDirectory);
                File metadataFile = new File(metsFilePath);
                return metadataFile;
            }
        } catch (DAOException e) {
            logger.error(e.toString());
        } catch (SwapException e) {
            logger.error(e.toString());
        } catch (IOException e) {
            logger.error(e.toString());
        } catch (InterruptedException e) {
            logger.error(e.toString());
        }
        return null;
    }

    // Filters for file searches
    public static FileFilter DirFilter = new FileFilter() {
        public boolean accept(File file) {
            return file.isDirectory();
        }
    };

    public Prefs getPrefs() {
        return prefs;
    }

    public String getCurrentSuffix() {
        return identifierSuffix;
    }

    private Document getMarcModsLoss(Document marcDoc, String modsString) {
        modsString = modsString.replaceAll("\"", "");
        ArrayList<String> trimStrings =
                new ArrayList<String>(Arrays.asList(new String[] { ".", ",", ":", ";", "(", ")", "[", "]", "{", "}", "\\", "/", "\t", "\n" }));
        Element record = marcDoc.getRootElement();
        if (record == null) {
            return null;
        }

        Document missingElementsDoc = new Document(new Element("Lost-in-MarcMods-Conversion"));
        // String modsString = CommonUtils.getStringFromDocument(modsDoc, "utf-8");
        // modsString = modsString.replaceAll("\"", "");

        Iterator<Content> descendant = record.getDescendants();
        while (descendant.hasNext()) {
            Element ele = null;
            Object obj = descendant.next();
            if (obj instanceof Element) {
                ele = (Element) obj;
                String marcContent = ele.getText();
                if (marcContent != null && !marcContent.isEmpty()) {
                    try {
                        marcContent = marcContent.trim();
                        while (trimStrings.contains(marcContent.substring(0, 1)) && marcContent.length() > 1) {
                            marcContent = marcContent.substring(1);
                            marcContent = marcContent.trim();
                        }
                        while (trimStrings.contains(marcContent.substring(marcContent.length() - 1, marcContent.length()))
                                && marcContent.length() > 1) {
                            marcContent = marcContent.substring(0, marcContent.length() - 1);
                            marcContent = marcContent.trim();
                        }
                        if (modsString.contains(marcContent.replaceAll("\"", ""))) {
                            // marc field is contained in the mods doc
                        } else {
                            Element parentElement = ele.getParentElement();
                            if (parentElement != null && parentElement != record) {
                                // The element has a parent that is not the root element, which we should log as well
                                Element parentClone = (Element) parentElement.clone();
                                parentClone.removeContent();
                                missingElementsDoc.getRootElement().addContent(parentClone);
                                parentClone.addContent((Element) ele.clone());
                            } else {
                                missingElementsDoc.getRootElement().addContent((Element) ele.clone());
                            }
                        }
                    } catch (IndexOutOfBoundsException e) {
                        continue;
                    }
                }

            }
        }
        return missingElementsDoc;
    }

    public String getId() {
        // TODO Auto-generated method stub
        return null;
    }

    public String createAtstsl(String myTitle, String autor) {
        String myAtsTsl = "";
        if (autor != null && !autor.equals("")) {
            /* autor */
            if (autor.length() > 4) {
                myAtsTsl = autor.substring(0, 4);
            } else {
                myAtsTsl = autor;
                /* titel */
            }

            if (myTitle.length() > 4) {
                myAtsTsl += myTitle.substring(0, 4);
            } else {
                myAtsTsl += myTitle;
            }
        }

        /*
         * -------------------------------- bei Zeitschriften Tsl berechnen --------------------------------
         */
        // if (gattung.startsWith("ab") || gattung.startsWith("ob")) {
        if (autor == null || autor.equals("")) {
            myAtsTsl = "";
            StringTokenizer tokenizer = new StringTokenizer(myTitle);
            int counter = 1;
            while (tokenizer.hasMoreTokens()) {
                String tok = tokenizer.nextToken();
                if (counter == 1) {
                    if (tok.length() > 4) {
                        myAtsTsl += tok.substring(0, 4);
                    } else {
                        myAtsTsl += tok;
                    }
                }
                if (counter == 2 || counter == 3) {
                    if (tok.length() > 2) {
                        myAtsTsl += tok.substring(0, 2);
                    } else {
                        myAtsTsl += tok;
                    }
                }
                if (counter == 4) {
                    if (tok.length() > 1) {
                        myAtsTsl += tok.substring(0, 1);
                    } else {
                        myAtsTsl += tok;
                    }
                }
                counter++;
            }
        }
        /* im ATS-TSL die Umlaute ersetzen */

        myAtsTsl = myAtsTsl.replaceAll("[\\W]", "");
        return myAtsTsl;
    }

}
