"""
Dynamic component registry for Mark 3.1.
Maps logical names to file paths and handles dynamic imports.
"""

import json
import importlib.util
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

class ComponentRegistry:
    """
    Central registry for all dynamic components.
    
    Usage:
        registry = ComponentRegistry()
        MomentumStrategy = registry.get('strategies', 'momentum')
        strategy = MomentumStrategy(config)
    """
    
    def __init__(self, registry_path: str = 'registry.json'):
        self.registry_path = Path(__file__).parent.parent.parent / registry_path
        self._components: Dict[str, Any] = {}
        self._mapping: Dict[str, Dict[str, str]] = {}
        self._load_registry()
    
    def _load_registry(self) -> None:
        """Load registry mapping from JSON file."""
        try:
            with open(self.registry_path, 'r') as f:
                self._mapping = json.load(f)
            logger.info(f"Loaded registry with {sum(len(v) for v in self._mapping.values())} components")
        except FileNotFoundError:
            logger.error(f"Registry file not found: {self.registry_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid registry JSON: {e}")
            raise
    
    def get(self, component_type: str, name: str) -> Any:
        """
        Get a component class by type and name.
        
        Args:
            component_type: e.g., 'strategies', 'data', 'risk'
            name: e.g., 'momentum', 'fetcher', 'manager'
        
        Returns:
            The imported module or class
        """
        cache_key = f"{component_type}:{name}"
        
        # Return cached if exists
        if cache_key in self._components:
            return self._components[cache_key]
        
        # Look up path
        try:
            rel_path = self._mapping[component_type][name]
        except KeyError:
            raise ImportError(f"Component {component_type}/{name} not found in registry")
        
        # Convert to absolute path
        base_dir = Path(__file__).parent.parent.parent
        full_path = base_dir / rel_path
        
        if not full_path.exists():
            raise ImportError(f"File not found: {full_path}")
        
        # Dynamic import
        spec = importlib.util.spec_from_file_location(name, full_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load module from {full_path}")
        
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Find the class (assumes class name matches or is same as name)
        # Convert 'momentum' to 'Momentum' or 'MomentumStrategy'
        class_candidates = [
            name.title(),                    # Momentum
            name.title() + 'Strategy',       # MomentumStrategy
            name.upper(),                    # MOMENTUM
        ]
        
        class_obj = None
        for candidate in class_candidates:
            if hasattr(module, candidate):
                class_obj = getattr(module, candidate)
                break
        
        # Fallback: look for any class ending with 'Strategy'
        if class_obj is None:
            for attr_name in dir(module):
                if attr_name.endswith('Strategy'):
                    class_obj = getattr(module, attr_name)
                    break
        
        if class_obj is None:
            raise ImportError(f"No class found in {full_path}")
        
        # Cache and return
        self._components[cache_key] = class_obj
        logger.debug(f"Loaded {component_type}/{name} from {rel_path}")
        return class_obj
    
    def reload(self) -> None:
        """Force reload registry and clear cache."""
        self._components.clear()
        self._load_registry()
        logger.info("Registry reloaded")
    
    def list_components(self, component_type: Optional[str] = None) -> Dict[str, list]:
        """List all available components."""
        if component_type:
            return {component_type: list(self._mapping.get(component_type, {}).keys())}
        
        return {k: list(v.keys()) for k, v in self._mapping.items()}