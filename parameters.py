from lxml import etree  
import json
from utils.migration_logging import logger

def extract(tree, twb_file_name):
    columns = {}
    nodes = []
    try:
        nodes = tree.xpath("//column[@param-domain-type]")
    except:
        logger.warn(f"could not find parameter columns")
        pass

    for node in nodes:        
        try: 
            for col in nodes:
                name = col.attrib.get('name').strip('[]')
                columns[f'{twb_file_name}_{name}'] = {
                    'source_reporting_system' : 'tableau',
                    'source': twb_file_name,
                    'raw' : etree.tostring(col, pretty_print=True).decode(),
                    'caption': col.attrib.get('caption'),
                    'datatype': col.attrib.get('datatype'),
                    'default-format': col.attrib.get('default-format'),
                    'name': col.attrib.get('name').strip('[]'),
                    'param-domain-type': col.attrib.get('param-domain-type'),
                    'role': col.attrib.get('role'),
                    'type': col.attrib.get('type'),
                    'value': col.attrib.get('value')
                }

                range_elem = col.find('.//range')
                if range_elem is not None:
                    columns[f'{twb_file_name}_{name}']['granularity'] = range_elem.attrib.get('granularity')
                    columns[f'{twb_file_name}_{name}']['max'] = range_elem.attrib.get('max')
                    columns[f'{twb_file_name}_{name}']['min'] = range_elem.attrib.get('min')
                
        except Exception as e:
            logger.warn(f"could not process parameters for - {etree.tostring(node, pretty_print=True).decode().strip()}", extra = {"file" : twb_file_name})
            logger.exception(e)
            pass
      

    return list(columns.values())
    