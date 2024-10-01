from lxml import etree
from utils.migration_logging import logger

def extract(tree, twb_file_name):
    extract_refreshes = []
    try:
        extract_node = tree.find('.//extract')
    except:
        logger.warn(f"could not find extracts in {twb_file_name} tableau file")
        pass
    if extract_node is not None:
        count = ""
        units = ""
        count = extract_node.get('count')
        units = extract_node.get('units')
        try:
            extract_object_id = extract_node.get('_.fcp.ObjectModelExtractV2.true...object-id')
        except:
            logger.warn(f"could not find datasource in {twb_file_name} tableau file")
            pass
        refresh_node = extract_node.find('.//refresh')
        if refresh_node is not None:
            incremental_updates = ''
            increment_key = ''
            top_rows_inserted = ''
            sample_rows_inserted = ''
            try:
                incremental_updates = refresh_node.get('incremental-updates')
            except:
                pass   
            if incremental_updates and incremental_updates.lower() == 'true':
                increment_key = refresh_node.get('increment-key')
                table_name = ''
                connection_name = ''
                
                if extract_object_id is not None:
                    object_nodes = tree.xpath(".//object")
                    for object_node in object_nodes:
                        object_id = object_node.attrib.get('id','')
                        if object_id == extract_object_id:
                            table_name = object_node.attrib.get('caption','')
                            properties_nodes = object_node.xpath(".//properties")
                            target_properties = [prop for prop in properties_nodes if prop.attrib.get('context') == '']
                            if target_properties:
                                relation_node = target_properties[0].xpath(".//relation")
                                if relation_node:
                                    for rel_node in relation_node:
                                        temp_connection_name = rel_node.attrib.get('connection','')   
                                        temp_table_name = rel_node.attrib.get('name','')
                                        if temp_table_name == table_name:
                                            connection_name = temp_connection_name  

                extract_refreshes.append({
                    'source' : twb_file_name,
                    'raw' : etree.tostring(extract_node, pretty_print=True).decode(),
                    'increment_column' : increment_key.strip("[]"),
                    'refresh_type':"Incremental Refresh",
                    'top_rows_inserted': top_rows_inserted,
                    'table_name': table_name,
                    'connection_name' : connection_name
                })

            if not incremental_updates or incremental_updates.lower() == 'false':
                if count != '-1':
                    if units == 'records':      
                        try:
                            top_rows_inserted = str(count)
                        except:
                            pass
                    elif units == 'sample-records':
                        try:
                            sample_rows_inserted = str(count)
                        except:
                            pass
                else:
                        top_rows_inserted =''
                        sample_rows_inserted = ''
                extract_refreshes.append({
                'source' : twb_file_name,
                'raw' : etree.tostring(extract_node, pretty_print=True).decode(),
                'refresh_type':'Full Refresh',
                'top_rows_inserted' : top_rows_inserted,
                'sample_rows_inserted' : sample_rows_inserted
            })
            
    return extract_refreshes