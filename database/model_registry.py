"""
Model registry for Connect-Tron-2000.

This module ensures all models are properly registered with the SQLAlchemy metadata
in the correct order, preventing conflicts and circular dependencies.
"""
import logging
from sqlalchemy.exc import InvalidRequestError
from app.database.models import Base, get_db_engine
from app.utils.logger import get_logger

logger = get_logger(__name__)

class ModelRegistry:
    """Model registry for ensuring proper model initialization."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """Singleton pattern to ensure only one registry exists."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the model registry."""
        if not self._initialized:
            self._relationship_models = []
            self._entity_models = []
            self._initialized = True
    
    def register_relationship_model(self, table):
        """
        Register a relationship model with the registry.
        
        Args:
            table: SQLAlchemy Table object representing a relationship
        """
        if table.name not in [t.name for t in self._relationship_models]:
            self._relationship_models.append(table)
            logger.debug(f"Registered relationship model: {table.name}")
    
    def register_entity_model(self, model_class):
        """
        Register an entity model with the registry.
        
        Args:
            model_class: SQLAlchemy model class
        """
        if model_class.__name__ not in [m.__name__ for m in self._entity_models]:
            self._entity_models.append(model_class)
            logger.debug(f"Registered entity model: {model_class.__name__}")
    
    def initialize_models(self):
        """Initialize all models in the correct order."""
        engine = get_db_engine()
        
        # Create tables for relationship models first
        for table in self._relationship_models:
            try:
                if not table.exists(engine):
                    table.create(engine)
                    logger.info(f"Created relationship table: {table.name}")
                else:
                    logger.debug(f"Relationship table already exists: {table.name}")
            except InvalidRequestError as e:
                logger.warning(f"Error creating relationship table {table.name}: {e}")
                # Ensure it has extend_existing set
                if not hasattr(table, 'extend_existing') or not table.extend_existing:
                    table.extend_existing = True
                    logger.info(f"Set extend_existing=True for table {table.name}")
        
        # Create all other tables
        try:
            # Create only the tables for our registered entity models
            tables = [model.__table__ for model in self._entity_models]
            # Create only tables that don't already exist
            missing_tables = [t for t in tables if not t.exists(engine)]
            
            if missing_tables:
                Base.metadata.create_all(engine, tables=missing_tables)
                logger.info(f"Created {len(missing_tables)} entity tables")
            else:
                logger.debug("All entity tables already exist")
        except Exception as e:
            logger.error(f"Error creating entity tables: {e}")
    
    def register_models(self):
        """Register all models from the codebase."""
        # Import relationship models first
        from app.database.relationship_models import discovered_url_organizations, org_keywords
        self.register_relationship_model(discovered_url_organizations)
        self.register_relationship_model(org_keywords)
        
        # Then import entity models
        from app.database.models import Organization, Contact, DiscoveredURL
        self.register_entity_model(Organization)
        self.register_entity_model(Contact)
        self.register_entity_model(DiscoveredURL)

# Global instance
registry = ModelRegistry()

def initialize_all_models():
    """Initialize all models in the correct order."""
    registry.register_models()
    registry.initialize_models()
    return True 