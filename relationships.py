from lxml import etree
from utils.migration_logging import logger

#TO determine the cardinality of relationships
def _determine_relationship_type(first_unique, second_unique):
    if first_unique and second_unique:
        return "One-to-One"
    if first_unique and not second_unique:
        return "One-to-Many"
    if not first_unique and second_unique:
        return "Many-to-One"
    if not first_unique and not second_unique:
        return "Many-to-Many"
    else:
        return "None"

#extract relationships
def _extract_relationships(tree, twb_file_name):
    cardinality_relationships = []
    relationship_nodes = []
    try:
        relationship_nodes = tree.xpath('//relationships/relationship')
    except:
        logger.warn(f"could not find relationships in {twb_file_name} tableau file")
        pass

    for node in relationship_nodes:
        try:
            expressions = node.xpath(".//expression/@op")  
            inequality_expressions = node.xpath(".//expression/@_.fcp.InequalityRelationships.true...op") 
            operand1 = ''
            operand2 = ''
            relationship_expression = ''
            no_common_column = False
            try:
                if len(expressions) > 1 and len(expressions) == 3:
                    relationship_operator = expressions[0] if expressions else ""
                    operand1 = expressions[1].strip('[]') if expressions else ""
                    operand2 = expressions[2].strip('[]') if expressions else ""
                    relationship_expression = f'{operand1} {relationship_operator} {operand2}'
                    no_common_column = False
                elif inequality_expressions and len(inequality_expressions) == 3:
                    relationship_operator = inequality_expressions[0] if expressions else ""
                    operand1 = inequality_expressions[1].strip('[]') if expressions else ""
                    operand2 = inequality_expressions[2].strip('[]') if expressions else ""
                    relationship_expression = f'{operand1} {relationship_operator} {operand2}'
                    no_common_column = False
                else:
                    no_common_column = True
            except Exception as e:
                logger.warn(f"could not process relationships for - {etree.tostring(node, pretty_print=True).decode().strip()}", extra = {"file" : twb_file_name})
                logger.exception(e)
                pass
            first_endpoint_object_id =  node.xpath("./first-end-point/@object-id")
            second_endpoint_object_id =  node.xpath("./second-end-point/@object-id")
            object_nodes = tree.xpath(".//object")
            table1_name = ''
            table2_name = ''
            for object_node in object_nodes:
                object_id = object_node.attrib.get('id','')
                if object_id == (first_endpoint_object_id[0].strip('[]')):
                    table1_name = object_node.attrib.get('caption','')
                elif object_id == (second_endpoint_object_id[0].strip('[]')):
                    table2_name = object_node.attrib.get('caption','')
                if table1_name and table2_name:
                    break
            first_endpoint_unique = node.xpath("./first-end-point/@unique-key")
            second_endpoint_unique = node.xpath("./second-end-point/@unique-key")
            first_endpoint_guaranteed_value = node.xpath("./first-end-point/@guaranteed-value")
            second_endpoint_guaranteed_value = node.xpath("./second-end-point/@guaranteed-value")
            first_guaranteed_value = "All records match" if first_endpoint_guaranteed_value else "Some records match"
            second_guaranteed_value = "All records match" if second_endpoint_guaranteed_value else "Some records match"
            first_unique = True if first_endpoint_unique else False
            second_unique = True if second_endpoint_unique else False
            relationship_type = _determine_relationship_type(first_unique,second_unique)
            cardinality_relationships.append({
                'source_reporting_system' : 'tableau',
                'source' : twb_file_name,
                'raw' : etree.tostring(node, pretty_print=True).decode(),
                'table1_name' : table1_name,
                'table2_name' : table2_name,
                'relationship_expression':relationship_expression,
                'relationship_type': relationship_type,
                'table1_inferential_integrity': first_guaranteed_value,
                'table2_inferential_integrity' : second_guaranteed_value,
                'no_common_column' : no_common_column
            })

        except Exception as e:
            logger.warn(f"could not process relationships for - {etree.tostring(node, pretty_print=True).decode().strip()}", extra = {"file" : twb_file_name})
            logger.exception(e)
            pass
    return cardinality_relationships
        
def _extract_union_join(tree,twb_file_name):
    union_join_relationships = []
    try:
        object_nodes = tree.xpath('//object')
        for obj_node in object_nodes:
            properties_node = obj_node.find('properties')
            if properties_node is not None:
                for rel_node in properties_node.iter('relation'):
                    if rel_node.get('type') == 'union':
                        try:
                            union_name = rel_node.get('name')
                            table_relations = rel_node.xpath("./relation[@type='table']")
                            table_names = ', '.join(rel.attrib.get('name', '') for rel in table_relations)
                            union_join_relationships.append({
                                'source_reporting_system' : 'tableau',
                                'source' : twb_file_name,
                                'raw' : etree.tostring(rel_node, pretty_print=True).decode(),
                                'table1_name' : table_names,
                                'union': union_name
                            })
                        except:
                            logger.warn(f"could not process data for union")
                            pass
                    elif rel_node.get('type') == 'join':
                        try:
                            join_type = rel_node.get('join','')
                            clause_nodes = rel_node.find('.//clause')
                            expression_ops = [expr.get('op','') for expr in clause_nodes.findall('.//expression')]
                            if len(expression_ops) > 2: #todo sayali check - test_new_filters as to why this broke
                                expression_operator = expression_ops[0]
                                operand1 = expression_ops[1]
                                operand2 = expression_ops[2]
                                table1_name = operand1.split('[')[1].split(']')[0]
                                table2_name = operand2.split('[')[1].split(']')[0]
                                union_join_relationships.append({
                                    'source_reporting_system' : 'tableau',
                                    'source' : twb_file_name,
                                    'raw' : etree.tostring(clause_nodes, pretty_print=True).decode(),
                                    'table1_name' : table1_name,
                                    'table2_name' : table2_name,
                                    'join_type': join_type,
                                    'relationship_expression' : f'{operand1}{expression_operator}{operand2}'
                                })
                        except:
                            logger.warn(f"could not process data for join")
                            pass
    except Exception as e:
            logger.warn(f"could not process join or union")
            logger.exception(e)
            pass
    return union_join_relationships

def extract(tree, twb_file_name):
    relationships=[]
    cardinality_relationships=[]
    union_join_relationships=[]

    union_join_relationships = _extract_union_join(tree,twb_file_name)
    relationships.extend(union_join_relationships)

    cardinality_relationships = _extract_relationships(tree, twb_file_name)
    relationships.extend(cardinality_relationships)

    return relationships
