from lxml import etree
from utils.migration_logging import logger

def extract(tree, twb_file_name):
    stories = []
    try:

        # get a list of all referenced entities in story points and their types for further reference
        referenced_entity_type_dict = {}
        for window in tree.findall('.//window'):
            name = window.get('name')
            class_ = window.get('class')
            referenced_entity_type_dict[name] = class_

        # Finding all stories
        nodes = tree.xpath("//dashboard[@type='storyboard']")

        for story in nodes:
            # check how the flipboard is styled
            flipboard = story.find(".//flipboard")
            nav_type = flipboard.get('nav-type') if flipboard is not None else None

            story_name = story.get('name')
            # Finding story points within each dashboard
            story_points = story.xpath(".//story-point")

            for sp in story_points:
                story_point_caption = sp.get('caption')
                
                for tuple_tag in sp.xpath('.//tuple'):
                    tuple_tag.getparent().remove(tuple_tag)

                stories.append({
                    'source_reporting_system' : 'tableau',
                    'source': twb_file_name,
                    'story_name': story_name,
                    'story_flipboad_style':nav_type,
                    'storypoint_caption': story_point_caption,
                    'referenced_entity_caption': sp.get('captured-sheet', ''),
                    'referenced_entity_type': referenced_entity_type_dict.get(sp.get('captured-sheet', '')),
                    'raw':etree.tostring(sp, pretty_print=True).decode()
                })

    except Exception as e:
        logger.warn(f"could not process stories for - {etree.tostring(sp, pretty_print=True).decode().strip()}", extra = {"file" : twb_file_name})
        logger.exception(e)
        pass
    
    return stories
    