package de.intranda.goobi.plugins.utils;

import static org.junit.Assert.*;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import de.intranda.goobi.plugins.utils.DocTypeConfiguration.DocTypeConfigurationException;

public class DocTypeConfigurationTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {
        String url = "http://aleph.csic.es/OAI?verb=GetRecord\\&amp;metadataPrefix=marc21\\&amp;identifier=oai:csic.aleph:DOC01-";
        try {
            XMLConfiguration pluginConfig = new XMLConfiguration("/opt/digiverso/goobi/config/plugin_CSICOAIImport.xml");
            DocTypeConfiguration docTypes = new DocTypeConfiguration(pluginConfig, url);
            
            boolean archive = false;
            boolean multipart = false;
            String typeOfResource = "text";
            
            assertEquals(docTypes.getDocType(typeOfResource, multipart, archive), "Monograph");
            assertNull(docTypes.getAnchorType(typeOfResource, multipart, archive));
            
            archive = true;
            assertEquals(docTypes.getDocType(typeOfResource, multipart, archive), "ArchiveDocument");
            assertNull(docTypes.getAnchorType(typeOfResource, multipart, archive));
            
            multipart = true;
            assertEquals(docTypes.getDocType(typeOfResource, multipart, archive), "ArchiveDocument");
            assertEquals(docTypes.getAnchorType(typeOfResource, multipart, archive), "Fonds");
            
            archive=false;
            assertEquals(docTypes.getDocType(typeOfResource, multipart, archive), "Volume");
            assertEquals(docTypes.getAnchorType(typeOfResource, multipart, archive), "MultiVolumeWork");
            
            typeOfResource = "cartographic";
            assertEquals(docTypes.getDocType(typeOfResource, multipart, archive), "Map");
            assertEquals(docTypes.getAnchorType(typeOfResource, multipart, archive), "MultiVolumeWork");
            
        } catch (ConfigurationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        } catch (DocTypeConfigurationException e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
