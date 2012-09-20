package de.intranda.goobi.plugins;

import java.awt.event.InputEvent;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.xeoh.plugins.base.annotations.PluginImplementation;

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
import org.jdom.output.XMLOutputter;
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
import de.sub.goobi.Import.ImportOpac;
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
	private static final String VERSION = "1.0.20120920";
	private static final String XSLT_PATH = ConfigMain.getParameter("xsltFolder") + "MARC21slim2MODS3.xsl";
	// private static final String XSLT_PATH = "resources/" + "MARC21slim2MODS3.xsl";
	// private static final String MODS_MAPPING_FILE = "resources/" + "mods_map.xml";
	private static final String MODS_MAPPING_FILE = ConfigMain.getParameter("xsltFolder") + "mods_map.xml";
	private static final String TEMP_DIRECTORY = ConfigMain.getParameter("tempfolder");

	private static final String idPrefix = "";
	public final File exportFolder = new File(ConfigPlugins.getPluginConfig(this).getString("importFolder", "/opt/digiverso/ftp-import/"));
	public final String projectIdentifierTitle = (ConfigPlugins.getPluginConfig(this).getString("projectIdentifier", "projectIdentifier"));
	public final String outputEncoding = (ConfigPlugins.getPluginConfig(this).getString("outputEncoding", "iso-8859-15"));
	public final String inputEncoding = (ConfigPlugins.getPluginConfig(this).getString("inputEncoding", "utf-8"));
	public final String conversionEncoding = (ConfigPlugins.getPluginConfig(this).getString("conversionEncoding", "iso-8859-15"));

	private String data;

	private Prefs prefs;
	private File importFolder = null; // goobi temp folder

	private Map<String, String> marcStructTypeMap = new HashMap<String, String>();
	private Map<String, String> anchorMap = new HashMap<String, String>();
	private Map<String, String> topStructMap = new HashMap<String, String>();
	private Map<String, String> logicalStructTypeMap = new HashMap<String, String>();
	private Map<String, String> projectsCollectionsMap = new HashMap<String, String>();
	private Map<String, File> recordImageMap = new HashMap<String, File>();

	private String currentIdentifier;
	private String currentTitle;
	private String currentAuthor;
	private List<String> currentCollectionList;

	private boolean deleteTempFiles = false;
	private boolean copyImages = true;
	private boolean deleteOriginalImages = true;
	private boolean updateExistingRecords = true;

	public CSICOAIImport() {
		marcStructTypeMap.put("?monographic", "Monograph");
		// map.put("?continuing", "Periodical");
		marcStructTypeMap.put("?continuing", "PeriodicalVolume");
		// map.put("?multipart monograph", "MultiVolumeWork");
		marcStructTypeMap.put("?multipart monograph", "Volume");
		marcStructTypeMap.put("?single unit", "Monograph");
		// map.put("?integrating resource", "MultiVolumeWork");
		marcStructTypeMap.put("?integrating resource", "Volume");
		// map.put("?serial", "Periodical");
		marcStructTypeMap.put("?serial", "SerialVolume");
		marcStructTypeMap.put("?cartographic", "Map");
		marcStructTypeMap.put("?notated music", null);
		marcStructTypeMap.put("?sound recording-nonmusical", null);
		marcStructTypeMap.put("?sound recording-musical", null);
		marcStructTypeMap.put("?moving image", null);
		marcStructTypeMap.put("?three dimensional object", null);
		marcStructTypeMap.put("?software, multimedia", null);
		marcStructTypeMap.put("?mixed material", null);

		anchorMap.put("Monograph", "Series");
		anchorMap.put("Manuscript", "Series");
		anchorMap.put("SingleManuscript", "Series");
		anchorMap.put("SerialVolume", "Series");
		anchorMap.put("Volume", "MultiVolumeWork");
		anchorMap.put("PeriodicalVolume", "Periodical");

		logicalStructTypeMap.put("MANUSCRIPT", "Manuscript");
		logicalStructTypeMap.put("DOCUMENT", "MultiVolumeWork");
		logicalStructTypeMap.put("ITEM", "Volume");
		logicalStructTypeMap.put("MONOGRAPH", "Monograph");
		logicalStructTypeMap.put("JOURNAL", "Perodical");
		logicalStructTypeMap.put("VOLUME", "PeriodicalVolume");

		topStructMap.put("MultiVolumeWork", "Volume");
		topStructMap.put("Periodical", "PeriodicalVolume");
		topStructMap.put("Series", "SerialVolume");

		projectsCollectionsMap.put("0001_POQ", "BIBLIOTECAS#Museo Nacional de Ciencias Naturales (Biblioteca)");
		projectsCollectionsMap.put("0005_BETN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		// projectsCollectionsMap.put("0006_PMSC", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		// projectsCollectionsMap.put("0006_PMSC_G_EEA", "BIBLIOTECAS#Centro de Estudios árabes GR-EEA");
		projectsCollectionsMap.put("0007_PCTN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		// projectsCollectionsMap.put("0008_PCTN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		// projectsCollectionsMap.put("0009_VCTN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		projectsCollectionsMap.put("0010_CMTN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		projectsCollectionsMap.put("0012_CIP", "BIBLIOTECAS#Museo Nacional de Ciencias Naturales (Biblioteca)");
		projectsCollectionsMap.put("0013_JAE", "BIBLIOTECAS#Museo Nacional de Ciencias Naturales (Biblioteca)");
		// projectsCollectionsMap.put("0014_FMTN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");
		// projectsCollectionsMap.put("0015_FAG", "BIBLIOTECAS#Centro de Estudios árabes GR-EEA");
		// projectsCollectionsMap.put("0016_FAAD", "BIBLIOTECAS#Estación Experimental Aula Dei (Biblioteca) ");
		// projectsCollectionsMap.put("0017_FACN", "BIBLIOTECAS#Museo Nacional de Ciencias Naturales (Biblioteca)");
		// projectsCollectionsMap.put("0018_ACN_PC","ARCHIVOS#Museo Nacional de Ciencias Naturales (Archivo)#Fondo Personal Científico#Ignacio Bolivar y Urrutia");
		projectsCollectionsMap.put("0030_CETN", "BIBLIOTECAS#Centro de Ciencias Humanas y Sociales (Biblioteca Tomás Navarro Tomás)");

	}

	@Override
	public List<ImportObject> generateFiles(List<Record> records) {
		ArrayList<ImportObject> ret = new ArrayList<ImportObject>();

		for (Record r : records) {
			logger.info("Processing " + r.getId());
			// Data conversion
			data = r.getData();
			File imageDir = recordImageMap.get(r.getId());
			currentIdentifier = r.getId();

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

	/**
	 * Uses image directories to retrieve metadata via oai
	 * 
	 */
	@Override
	public List<Record> generateRecordsFromFilenames(List<String> filenames) {

		List<Record> recordList = new ArrayList<Record>();

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
			
			// Get the aleph-Identifier from the filename
			parts = dirName.split("_");
			String idString = null;
			String idSuffix = null;

			if (parts == null || parts.length < 1) {
				continue;
			}

			if (parts[0].contentEquals("M")) {
				idString = parts[1];
			} else if (parts.length > 1) {
				idString = parts[0];
			}

			int last = parts.length - 1;
			if (!parts[last].contentEquals(idString) || parts[last].contentEquals("V00")) {
				// We have a volume number or pieceDesignation
				idSuffix = parts[last];
			}

			if (idString == null) {
				continue;
			}

			idString = idPrefix + idString;
			logger.debug("Found record id " + idString);
			String marcRecord = getMarcRecordFromOAI(idString);
			if (marcRecord == null) {
				logger.error("Unable to retrieve marc-record " + idString + ".");
				continue;
			}

			Record record = new Record();
			record.setId(idString + "_" + idSuffix);
			record.setData(marcRecord);
			// recordList.add(record);
			recordImageMap.put(record.getId(), recordDir);

			File oldFile = searchForExistingData(record.getId());
			if (oldFile != null) {
				if (updateExistingRecords) {
					logger.info("Found existing record. Updating.");
					if (!updateOldRecord(record, oldFile)) {
						Helper.setFehlerMeldung("Error updating record " + record.getId());
					} else {
						Helper.setMeldung("Updated process " + record.getId());
					}
				} else {
					Helper.setFehlerMeldung("Cannot import record " + record.getId() + " : A process with this title already exists.");
				}
			} else {
				recordList.add(record);
			}
		}

		return recordList;
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
			copyImageFiles(imageDir);
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

					// File[] files = tempDir.listFiles();
					// if (files != null) {
					// for (File file : files) {
					// File newFile = new File(metadataDir, file.getName());
					// if (file.isDirectory()) {
					// if(newFile.isDirectory()) {
					// CommonUtils.m
					// }
					// // org.apache.commons.io.FileUtils.copyDirectory(file, newFile);
					// } else if (file.isFile()) {
					// int counter = 0;
					// while (newFile.isFile()) {
					// String fileNameTrunk = newFile.getName().substring(0, newFile.getName().indexOf("."));
					// String fileNameExt = newFile.getName().substring(newFile.getName().indexOf("."));
					// newFile = new File(newFile.getParent(), fileNameTrunk + counter + fileNameExt);
					// counter++;
					// }
					// org.apache.commons.io.FileUtils.copyDirectory(file, newFile);
					// }
					// }
					// }
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

		String urlPrefix = "http://aleph.csic.es/OAI?verb=GetRecord&metadataPrefix=marc21&identifier=oai:csic.aleph:MAD01-";
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

				// Determine the root docstruct type
				String dsType = null;
				String dsAnchorType = null;

				// handle TypeOfResource
				List<Element> eleTypeOfResourceList = eleMods.getChildren("typeOfResource", null);
				if (eleTypeOfResourceList != null) {
					for (Element eleTypeOfResource : eleTypeOfResourceList) {
						if ("yes".equals(eleTypeOfResource.getAttributeValue("manuscript"))) {
							// Manuscript
							dsType = "SingleManuscript";
						} else if (marcStructTypeMap.get("?" + eleTypeOfResource.getTextTrim()) != null) {
							dsType = marcStructTypeMap.get("?" + eleTypeOfResource.getTextTrim());
						} else {
							dsType = "Monograph";
						}
					}
				}

				// handle issuance/frequency
				List<Element> eleOriginInfoList = eleMods.getChildren("originInfo", null);
				if (eleOriginInfoList != null) {
					for (Element eleOriginInfo : eleOriginInfoList) {
						Element eleIssuance = eleOriginInfo.getChild("issuance", null);
						if (eleIssuance != null && marcStructTypeMap.get("?" + eleIssuance.getTextTrim()) != null) {
							if (!dsType.contains("Manuscript") && marcStructTypeMap.get("?" + eleIssuance.getTextTrim()) != null) {
								dsType = marcStructTypeMap.get("?" + eleIssuance.getTextTrim());
							}
						}
						Element eleFrequency = eleOriginInfo.getChild("frequency", null);
						if (eleFrequency != null && eleFrequency.getValue() != null && !eleFrequency.getValue().isEmpty()) {
							// it has a frequency, therefore gets an anchor
							dsAnchorType = anchorMap.get(dsType);
							if (dsAnchorType == null) {
								dsAnchorType = "Series";
							}
						}
					}
				}

				// handle relatedSeries
				List<Element> eleRelatedSeriesList = eleMods.getChildren("relatedItem", null);
				if (eleRelatedSeriesList != null) {
					for (Element eleRelatedSeries : eleRelatedSeriesList) {

						if (eleRelatedSeries != null && eleRelatedSeries.getAttribute("type") != null
								&& eleRelatedSeries.getAttribute("type").getValue().contentEquals("series")) {
							if (dsAnchorType == null) {
								dsAnchorType = "Series";
							}
						}
					}
				}

				// if we still don't have an anchorType, but dsType requires one, create an appropriate one
				if (dsAnchorType == null) {
					if (dsType.contentEquals("PeriodicalVolume")) {
						dsAnchorType = "Periodical";
					} else if (dsType.contentEquals("Volume")) {
						dsAnchorType = "MultiVolumeWork";
					}
				} else { // There is an anchor
					if (dsType.contentEquals("SingleManuscript")) {
						dsType = "Manuscript";
					} else if (dsType.contentEquals("Monograph")) {
						dsType = "SerialVolume";
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
				}

				DocStruct dsBoundBook = dd.createDocStruct(prefs.getDocStrctTypeByName("BoundBook"));
				dd.setPhysicalDocStruct(dsBoundBook);
				// Collect MODS metadata
				ModsUtils.parseModsSection(MODS_MAPPING_FILE, prefs, dsVolume, dsBoundBook, dsAnchor, eleMods);
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
						if (volumeName.contentEquals("Monograph")) {
							dsVolume.setType(prefs.getDocStrctTypeByName("SerialVolume"));
						}
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
						if (volumeName.contentEquals("Manuscript")) {
							dsVolume.setType(prefs.getDocStrctTypeByName("SingleManuscript"));
						}
					}
				} catch (TypeNotAllowedAsChildException e) {
					logger.error("Child Type not allowed", e);
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
						if (dsAnchor != null) {
							dsAnchor.addMetadata(mdCollection);
						}
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

	@Override
	public String getImportFolder() {
		return importFolder.getAbsolutePath() + File.separator;
	}

	@Override
	public String getProcessTitle() {
		String title = "";
		if (StringUtils.isNotBlank(currentTitle)) {
			String atstsl = new ImportOpac().createAtstsl(currentTitle, currentAuthor);
			if (atstsl != null) {
				title = new ImportOpac().createAtstsl(currentTitle, currentAuthor).toLowerCase() + "_" + currentIdentifier + ".xml";
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
		// filenameList.add("Dummy");
		// filenameList.add(exportFolder.getAbsolutePath());
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
								filenameList.add(project + "::\t" + recordDir.getName());
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
			String id = getProcessTitle();
			if(id != null) {
				id = id.replace(".xml", "");
				if(importFolder.isDirectory() && importFolder.listFiles() != null) {
					for (File file : importFolder.listFiles()) {
						if(file.getName().contains(id)) {
								CommonUtils.deleteAllFiles(file);
						}
					}
				}
						
			}
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

		// get temp dir
		File tempDir = new File(importFolder, getProcessTitle().replace(".xml", ""));
		File tempImageDir = new File(tempDir, "images");
		File tempTiffDir = new File(tempImageDir, getProcessTitle().replace(".xml", "") + "_tif");
//		File tempOrigDir = new File(tempImageDir, "orig_" + getProcessTitle().replace(".xml", "") + "_tif");
		tempTiffDir.mkdirs();

		// parse all image Files and write them into new Files in the import
		// directory
		List<File> images = Arrays.asList(imageDir.listFiles(CommonUtils.ImageFilter));
		try {
			for (File imageFile : images) {
				String filename = imageFile.getName();
				if (!deleteOriginalImages) {
					copyFile(imageFile, new File(tempTiffDir, filename));
					logger.debug("Copying image " + filename);
				} else {
					CommonUtils.moveFile(imageFile, new File(tempTiffDir, filename), true);
//					if (!imageFile.renameTo(new File(tempTiffDir, filename))) {
//						copyFile(imageFile, new File(tempTiffDir, filename));
//						imageFile.delete();
//					}
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
	 * 
	 * @param text
	 * @return
	 * @throws IOException
	 */
	private String convertTextToMarcXml(String text) throws IOException {
		if (StringUtils.isNotEmpty(text)) {
			Document doc = new Document();
			text = text.replace((char) 0x1E, ' ');
			BufferedReader reader = new BufferedReader(new StringReader(text));
			Element eleRoot = new Element("collection");
			doc.setRootElement(eleRoot);
			Element eleRecord = new Element("record");
			eleRoot.addContent(eleRecord);
			String str;
			while ((str = reader.readLine()) != null) {
				if (str.toUpperCase().startsWith("=LDR")) {
					// Leader
					Element eleLeader = new Element("leader");
					eleLeader.setText(str.substring(7));
					eleRecord.addContent(eleLeader);
				} else if (str.length() > 2) {
					String tag = str.substring(1, 4);
					if (tag.startsWith("00") && str.length() > 6) {
						// Control field
						str = str.substring(6);
						Element eleControlField = new Element("controlfield");
						eleControlField.setAttribute("tag", tag);
						eleControlField.addContent(str);
						eleRecord.addContent(eleControlField);
					} else if (str.length() > 6) {
						// Data field
						String ind1 = str.substring(6, 7);
						String ind2 = str.substring(7, 8);
						str = str.substring(8);
						Element eleDataField = new Element("datafield");
						eleDataField.setAttribute("tag", tag);
						eleDataField.setAttribute("ind1", !ind1.equals("\\") ? ind1 : "");
						eleDataField.setAttribute("ind2", !ind2.equals("\\") ? ind2 : "");
						Pattern p = Pattern.compile("[$]+[^$]+");
						Matcher m = p.matcher(str);
						while (m.find()) {
							String sub = str.substring(m.start(), m.end());
							Element eleSubField = new Element("subfield");
							eleSubField.setAttribute("code", sub.substring(1, 2));
							eleSubField.addContent(sub.substring(2));
							eleDataField.addContent(eleSubField);
						}
						eleRecord.addContent(eleDataField);
					}
				}
			}
			return new XMLOutputter().outputString(doc);
		}

		return null;
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

	/**
	 * returns the metadatafile meta.xml if a prozess of this name was found, null otherwise
	 * 
	 * @param processTitle
	 * @return
	 */
	private File searchForExistingData(String processTitle) {
		String metsFilePath, processDataDirectory;
		ProzessDAO dao = new ProzessDAO();

		try {
			List<Prozess> processList = dao.search("from Prozess where titel LIKE '%" + processTitle + "'");

			if (processList != null && !processList.isEmpty()) {
				Prozess p = processList.get(0);
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

	@Override
	public String getId() {
		return null;
	}

}
