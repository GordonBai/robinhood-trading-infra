"""Independent synthetic external plugins. They import only public contracts."""
from .providers import MeanReversionProvider, MomentumProvider

__all__ = ['MomentumProvider', 'MeanReversionProvider']
