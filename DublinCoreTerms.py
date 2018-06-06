import xml.sax.saxutils
from dublincore import dublinCoreMetadata

class DublinCore(dublinCoreMetadata):
    def __init__(self):
        dublinCoreMetadata.__init__(self)
        ### self.Relation = []
    
    def makeXML(self, schemaLocation, encapsulatingTag='metadata'):
        """
        This method transforms the class attribute data into standards
        compliant XML according to the guidlines laid out in 
        "Guidelines for implementing Dublin Core in XML" available online 
        at http://www.dublincore.org/documents/2003/04/02/dc-xml-guidelines/
        
        This method takes one mandatory argument, one optional argument and
        returns a string. The mandatory argument is the location of the XML
        schema which should be a fully qualified URL. The option arguments
        is the root tag with which to enclose and encapsulate all of the
        DC elements. The default is "metadata" but it can be overridden if
        needed.
        
        The output can be directed to a file or standard output. This RDF 
        data should be suitable for marking most documents including webpages.
        """
        #set XML declaration
        xmlOut = '<?xml version="1.0"?>\n'
        
        #open encapsulating element tag and deal with namespace and schema declarations
        xmlOut += '''\n<%s
    xmlns="http://example.org/myapp/"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="%s"
    xmlns:dc="http://purl.org/dc/elements/1.1/"
    xmlns:dcterms="http://purl.org/dc/terms/">\n\n''' % (encapsulatingTag, schemaLocation)

        with open('mapfiles/dcelements.txt','r') as f:
                    dcelements = f.read().splitlines()
        for dcelem in dcelements :
                val=getattr(self, dcelem.capitalize())
                if hasattr(self,dcelem.capitalize()) and val :
                     xmlOut += '\t<dc:%s>%s</dc:%s>\n' % (dcelem,xml.sax.saxutils.escape(val),dcelem)

        with open('mapfiles/dcterms.txt','r') as f:
            dcterms = f.read().splitlines()
        for dcterm in dcterms :
            val=getattr(self, dcterm.capitalize())
            if hasattr(self,dcterm.capitalize()) and val :
                xmlOut += '\t<dcterms:%s>%s</dcterms:%s>\n' % (dcterm,xml.sax.saxutils.escape(val),dcterm)

        #if the Alternative term is set, make the dcterms:alternative tag
        if self.Alternative:
            xmlOut += '\t<dcterms:alternative>%s</dcterms:alternative>\n' % xml.sax.saxutils.escape(self.Alternative)
        
        #close encapsulating element tag
        xmlOut += '</metadata>\n'
        
        return xmlOut
