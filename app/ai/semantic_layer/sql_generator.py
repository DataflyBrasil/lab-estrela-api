from typing import List, Optional, Dict
from app.ai.semantic_layer.loader import loader

class SqlGenerator:
    def __init__(self):
        self.loader = loader

    def generate_query(self, metric_name: str, dimension_name: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, filters: Optional[Dict] = None) -> str:
        metric = self.loader.get_metric(metric_name)
        if not metric:
            raise ValueError(f"Metric '{metric_name}' not found.")
        
        select_clause = [f"{metric['sql']} AS {metric_name}"]
        from_clause = [] # To be determined based on joins
        group_by_clause = []
        where_clause = []
        
        # Determine the primary table for the metric
        main_table = "SMM" if "SMM." in metric ['sql'] else "OSM"
        from_clause.append(f"{main_table} AS {main_table}")
        
        joined_tables = {main_table}
        
        # --- Handle Date Filtering ---
        if start_date or end_date:
            # We need the date dimension definition to know which column to filter
            date_dim = self.loader.get_dimension('date')
            if date_dim:
                # Ensure date dimension tables are joined
                if 'join_path' in date_dim:
                     # Join logic for date dimension if needed (usually OSM is already there)
                     pass

                # Prefer using the raw column if possible for performance, or the dimension SQL
                # The dimension 'date' sql is "CAST(OSM.OSM_DTHR AS DATE)"
                # For filtering, we might want the raw column "OSM.OSM_DTHR" to avoid casting issues in WHERE if index usage is important
                # But to be safe and consistent with semantic layer:
                date_col = "OSM.OSM_DTHR" # Hardcoded optimization or derive from dimension['sql']
                
                if start_date:
                    where_clause.append(f"{date_col} >= '{start_date}'")
                if end_date:
                    where_clause.append(f"{date_col} <= '{end_date} 23:59:59'")
                
                # Ensure OSM is joined if it's not the main table
                if "OSM" not in joined_tables:
                     from_clause.append("INNER JOIN OSM AS OSM ON SMM.SMM_OSM = OSM.OSM_NUM AND SMM.SMM_OSM_SERIE = OSM.OSM_SERIE")
                     joined_tables.add("OSM")

        if dimension_name:
            dimension = self.loader.get_dimension(dimension_name)
            if not dimension:
                 raise ValueError(f"Dimension '{dimension_name}' not found.")
            
            select_clause.append(f"{dimension['sql']} AS {dimension_name}")
            group_by_clause.append(dimension['sql'])
            
            # Handle Joins for Dimension
            if 'join_path' in dimension:
                path = dimension['join_path']
                # path example: [OSM, STR] or [SMM, SMK]
                
                # Logic to join tables in the path
                for i in range(len(path) - 1):
                    source = path[i]
                    target = path[i+1]
                    
                    if target in joined_tables:
                        continue
                        
                    # Find join definition in Source or Target entity
                    source_entity = self.loader.get_entity(source)
                    # target_entity = self.loader.get_entity(target) # Check if join is defined here (reverse)
                    
                    join_def = next((j for j in source_entity.get('joins', []) if j['target'] == target), None)
                    
                    if join_def:
                         from_clause.append(f"{join_def['type'].upper()} JOIN {target} AS {target} ON {join_def['condition']}")
                         joined_tables.add(target)
                    else:
                        # Try reverse join if needed, or simple error for now
                        # Check if we need to join implicit tables (e.g. SMM -> OSM)
                        if source == 'SMM' and target == 'OSM': # Known special case
                             from_clause.append(f"INNER JOIN {target} AS {target} ON SMM.SMM_OSM = OSM.OSM_NUM AND SMM.SMM_OSM_SERIE = OSM.OSM_SERIE")
                             joined_tables.add(target)
                        elif source == 'OSM' and target == 'SMM':
                             from_clause.append(f"INNER JOIN {target} AS {target} ON SMM.SMM_OSM = OSM.OSM_NUM AND SMM.SMM_OSM_SERIE = OSM.OSM_SERIE")
                             joined_tables.add(target)

        # Basic default join SMM to OSM if not already joined, as most logical connections go through OSM
        if "SMM" in joined_tables and "OSM" not in joined_tables:
             from_clause.append("INNER JOIN OSM AS OSM ON SMM.SMM_OSM = OSM.OSM_NUM AND SMM.SMM_OSM_SERIE = OSM.OSM_SERIE")
             joined_tables.add("OSM")

        # --- Apply Business Rules ---
        # Check if the metric has business_rules defined
        if 'business_rules' in metric and metric['business_rules']:
            for rule_name in metric['business_rules']:
                rule = self.loader.get_business_rule(rule_name)
                if rule and rule.get('filter_type') == 'WHERE':
                    # Check if rule requires a join
                    if 'requires_join' in rule:
                        required_table = rule['requires_join']
                        if required_table not in joined_tables:
                            # Need to join this table first
                            # Try to find join from OSM or SMM to this table
                            osm_entity = self.loader.get_entity('OSM')
                            join_def = next((j for j in osm_entity.get('joins', []) if j['target'] == required_table), None)
                            
                            if join_def:
                                from_clause.append(f"{join_def['type'].upper()} JOIN {required_table} AS {required_table} ON {join_def['condition']}")
                                joined_tables.add(required_table)
                            else:
                                # Try from SMM
                                smm_entity = self.loader.get_entity('SMM')
                                if smm_entity:
                                    join_def = next((j for j in smm_entity.get('joins', []) if j['target'] == required_table), None)
                                    if join_def:
                                        from_clause.append(f"{join_def['type'].upper()} JOIN {required_table} AS {required_table} ON {join_def['condition']}")
                                        joined_tables.add(required_table)
                    
                    # Add the filter condition
                    where_clause.append(rule['condition'])
        
        sql = f"SELECT {', '.join(select_clause)} FROM {' '.join(from_clause)}"
        
        if where_clause:
            sql += f" WHERE {' AND '.join(where_clause)}"
            
        if group_by_clause:
            sql += f" GROUP BY {', '.join(group_by_clause)}"

        return sql
