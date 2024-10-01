from lxml import etree
from utils.migration_logging import logger

def _get_sql_query(tree,datasource_connection_name):
    # Match connection name in fcp.objectModelEncapsulateLegacy and extract SQL query
    fcp_object_nodes = tree.findall('.//_.fcp.ObjectModelEncapsulateLegacy.true...relation')
    for fcp_node in fcp_object_nodes:
        connection_name=fcp_node.get('connection','')
        sql_query = fcp_node.text.strip() if fcp_node.text else None # Extract the text content if available
        if connection_name == datasource_connection_name:
            if sql_query:
                return sql_query
        else:
            return ""

def extract(tree, twb_file_name):
    datasources = {}
    named_nodes = []

    try:
        named_nodes = tree.xpath('//named-connections/named-connection')
    except:
        logger.warn(f"could not find connection")
        pass

    for named_node in named_nodes:
        try:
            for node in named_node:
                
                datasource = node.xpath('ancestor::datasource') 
                if len(datasource):
                    name = datasource[0].attrib.get('name', '').strip("[]")
                    caption = datasource[0].attrib.get('caption', '')  
                    connection_name = named_node.attrib.get('name', name)
                    datasources[f"{name}_{connection_name}"] = {   
                        'source_reporting_system' : 'tableau',
                        'source': twb_file_name, 
                        'raw' : etree.tostring(node, pretty_print=True).decode(),            
                        'caption': caption,
                        'name': name,
                        'connection_name': connection_name,
                        'class': node.attrib.get('class', ''),
                        'sql_query': _get_sql_query(tree, connection_name),
                        #file based
                        'filename': node.attrib.get('filename', ''),
                        'directory': node.attrib.get('directory', ''),
                        'password': node.attrib.get('password', ''),

                        # server details
                        'server': node.attrib.get('server', ''),
                        'dbname': node.attrib.get('dbname', ''),
                        'schema': node.attrib.get('schema', ''),
                        'warehouse': node.attrib.get('warehouse', ''),
                        'service': node.attrib.get('service', ''),
                        'username': node.attrib.get('username', ''),
                        'authentication_type': node.attrib.get('authentication', ''),
                        'odbc-connect-string-extras': node.attrib.get('odbc-connect-string-extras', ''),
                        'one-time-sql': node.attrib.get('one-time-sql', ''),   

                        #extract
                        'update-time': node.attrib.get('update-time', ''),   
                        'tablename': node.attrib.get('tablename', ''), 
                        'sslmode': node.attrib.get('sslmode', ''), 
                        'author-locale': node.attrib.get('author-locale', '')                                     
                    }

        except Exception as e:
            logger.warn(f"could not process datasources for - {etree.tostring(node, pretty_print=True).decode().strip()}", extra = {"file" : twb_file_name})
            logger.exception(e)
            pass
   
    return list(datasources.values())


    