from lxml import etree
from utils.migration_logging import logger

def extract(tree, twb_file_name):
    data = []
    try:        
        # Extracting style-rule elements for categorical fields
        style_rules = tree.xpath('//datasource//style-rule')        
        for node in style_rules:
            raw = etree.tostring(node, pretty_print=True).decode()
            dnode = node.xpath('ancestor::datasource')[0]
            element = node.get('element')
            for encoding in node.xpath('.//encoding'):
                attr = encoding.get('attr')
                field = encoding.get('field')
                encoding_type = encoding.get('type', '')
                for map_tag in encoding.xpath('.//map'):
                    to_value = map_tag.get('to')
                    for bucket in map_tag.xpath('.//bucket'):
                        bucket_value = bucket.text
                        data.append({
                            'source_reporting_system' : 'tableau',
                            'source': twb_file_name,
                            'worksheet':'',
                            'raw' : raw,
                            'encoding': element,
                            'formatting_attribute': attr,
                            'table_name' : dnode.get('name'),
                            'table_caption' : dnode.get('caption'),
                            'column_name': field,
                            'type':'categorical',
                            'color': to_value,
                            'bucket': bucket_value,
                            'encoding_type': encoding_type,
                            'encoding_palette':''
                        })        

        # Extracting style-rule elements for continous fields
        worksheets = tree.xpath('//worksheet')
        for worksheet in worksheets:
            
            element = worksheet.get('element')
            style_rules = worksheet.xpath('.//style-rule[@element="mark"]')
            for node in style_rules:
                color_encodings = node.xpath('.//encoding[@attr="color"]')
                for encoding in color_encodings:
                    color_palette = node.find('.//color-palette')                    
                    encoding_type = encoding.get('type', '')
                    encoding_palette = encoding.get('palette','')
                    attr = encoding.get('attr')
                    raw = etree.tostring(encoding, pretty_print=True).decode()
                    colors = None
                    if color_palette is not None:
                        colors = [color_tag.text for color_tag in color_palette.findall('.//color')]
                        encoding_palette = None
                    data.append({
                        'source_reporting_system' : 'tableau',
                        'source': twb_file_name,
                        'worksheet':worksheet.get('name'),
                        'raw' : raw,
                        'encoding': element,
                        'formatting_attribute': attr,
                        'table_name' : "", #todo - check with omkar
                        'table_caption' : "", #todo - check with omkar
                        'column_name': "", #todo - check with omkar
                        'type':'continous',
                        'color': colors,
                        'bucket': '',
                        'encoding_type': encoding_type,
                        'encoding_palette':encoding_palette
                        })

    except Exception as e:
        logger.warn(f"could not process columns for - {etree.tostring(node, pretty_print=True).decode().strip()}", extra = {"file" : twb_file_name})
        logger.exception(e)
        pass

    return data