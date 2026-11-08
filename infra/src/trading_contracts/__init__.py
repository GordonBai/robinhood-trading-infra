"""Neutral strategy contracts, without execution or strategy dependencies."""
from .exits import ExitAction, ExitContext, ExitRecommendation, ExitStrategy
from .strategy import (StrategyIdentity, StrategyInput, ProposalAction,
                       StrategyProposal, StrategyDecision, StrategyPlugin)

__all__ = ['ExitAction', 'ExitContext', 'ExitRecommendation', 'ExitStrategy',
           'StrategyIdentity', 'StrategyInput', 'ProposalAction', 'StrategyProposal',
           'StrategyDecision', 'StrategyPlugin']
