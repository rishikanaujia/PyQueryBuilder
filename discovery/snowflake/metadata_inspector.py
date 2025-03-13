# # pyquerybuilder/discovery/metadata_inspector.py
# """Inspector for discovering database metadata."""
# from typing import Dict, List, Any
#
#
# class MetadataInspector:
#     """Discovers and extracts database schema metadata."""
#
#     def __init__(self, connector):
#         """Initialize with database connector."""
#         self.connector = connector
#
#     def discover_schema(self, include_tables=None,
#                         exclude_tables=None, include_views=True):
#         """Discover database schema metadata.
#
#         Args:
#             include_tables: Optional list of tables to include
#             exclude_tables: Optional list of tables to exclude
#             include_views: Whether to include views
#
#         Returns:
#             Dictionary containing schema metadata
#         """
#         tables = self._discover_tables(include_views)
#         columns = self._discover_columns(tables)
#         relationships = self._discover_relationships(tables)
#
#         return {
#             "tables": tables,
#             "columns": columns,
#             "relationships": relationships
#         }