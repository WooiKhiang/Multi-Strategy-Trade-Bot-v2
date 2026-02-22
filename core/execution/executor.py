"""
Trade executor for Mark 3.1.
Places orders through Alpaca with proper order types and tracking.
"""

import logging
import uuid
from datetime import datetime
from typing import Optional, Dict, Any, Tuple
import threading
import time

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce, OrderType

from core.utils.registry import ComponentRegistry
from config.settings import settings

logger = logging.getLogger(__name__)

class Executor:
    """
    Executes trades through Alpaca.
    
    Handles:
    - Order placement (market/limit)
    - Order tracking
    - Fill confirmation
    - Partial fills
    """
    
    def __init__(self, paper: bool = True):
        # Initialize Alpaca trading client
        self.client = TradingClient(
            api_key=settings.ALPACA_TRADING_KEY,
            secret_key=settings.ALPACA_SECRET_KEY,
            paper=paper
        )
        
        # Get dependencies
        registry = ComponentRegistry()
        self.slippage = registry.get('execution', 'slippage')()
        
        # Track pending orders
        self.pending_orders = {}
        
        logger.info(f"Executor initialized (paper={paper})")
    
    def _generate_ticket_id(self) -> str:
        """Generate unique ticket ID."""
        return f"TKT-{uuid.uuid4().hex[:8].upper()}"
    
    def execute_entry(self, ticker: str, strategy: str,
                     price: float, quantity: int,
                     order_type: str = 'LIMIT',
                     time_in_force: str = 'day') -> Dict[str, Any]:
        """
        Execute an entry order.
        
        Args:
            ticker: Symbol to buy
            strategy: Strategy name
            price: Target price
            quantity: Number of shares
            order_type: 'LIMIT' or 'MARKET'
            time_in_force: 'day' or 'gtc'
        
        Returns:
            Dict with order details
        """
        ticket_id = self._generate_ticket_id()
        
        try:
            if order_type.upper() == 'MARKET':
                # Market order
                order_data = MarketOrderRequest(
                    symbol=ticker,
                    qty=quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
                order = self.client.submit_order(order_data)
                
                # For market orders, we get immediate fill (usually)
                fill_price = float(order.filled_avg_price) if order.filled_avg_price else price
                
            else:
                # Limit order
                order_data = LimitOrderRequest(
                    symbol=ticker,
                    limit_price=round(price, 2),
                    qty=quantity,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                )
                order = self.client.submit_order(order_data)
                
                # For limit orders, may not fill immediately
                if order.filled_at:
                    fill_price = float(order.filled_avg_price)
                else:
                    # Store for monitoring
                    self.pending_orders[order.id] = {
                        'ticket_id': ticket_id,
                        'ticker': ticker,
                        'strategy': strategy,
                        'expected_price': price,
                        'quantity': quantity,
                        'order_type': order_type,
                        'submitted_at': datetime.utcnow()
                    }
                    
                    logger.info(f"Limit order pending: {order.id} for {ticker}")
                    
                    return {
                        'ticket_id': ticket_id,
                        'order_id': order.id,
                        'status': 'PENDING',
                        'ticker': ticker,
                        'quantity': quantity,
                        'limit_price': price,
                        'message': 'Order submitted, waiting for fill'
                    }
            
            # Record execution
            self.slippage.record_execution(
                ticket_id=ticket_id,
                ticker=ticker,
                expected_price=price,
                actual_price=fill_price,
                expected_quantity=quantity,
                actual_quantity=quantity,  # Assume full fill for now
                order_type=order_type,
                side='BUY'
            )
            
            logger.info(f"Executed {order_type} BUY {quantity} {ticker} @ ${fill_price:.2f}")
            
            return {
                'ticket_id': ticket_id,
                'order_id': order.id,
                'status': 'FILLED',
                'ticker': ticker,
                'quantity': quantity,
                'fill_price': fill_price,
                'strategy': strategy,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Order failed for {ticker}: {e}")
            return {
                'ticket_id': ticket_id,
                'status': 'FAILED',
                'ticker': ticker,
                'error': str(e)
            }
    
    def execute_exit(self, ticker: str, quantity: int,
                    order_type: str = 'MARKET',
                    limit_price: Optional[float] = None,
                    reason: str = 'STRATEGY') -> Dict[str, Any]:
        """
        Execute an exit order.
        
        Args:
            ticker: Symbol to sell
            quantity: Number of shares
            order_type: 'MARKET' or 'LIMIT'
            limit_price: Required if order_type is 'LIMIT'
            reason: Why exiting (STOP_LOSS, TAKE_PROFIT, STRATEGY)
        """
        ticket_id = self._generate_ticket_id()
        
        try:
            if order_type.upper() == 'MARKET' or reason == 'STOP_LOSS':
                # Market order for stops and forced exits
                order_data = MarketOrderRequest(
                    symbol=ticker,
                    qty=quantity,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )
                order = self.client.submit_order(order_data)
                
                fill_price = float(order.filled_avg_price) if order.filled_avg_price else 0
                
            else:
                # Limit order for take profit
                if not limit_price:
                    raise ValueError("Limit price required for LIMIT exit")
                
                order_data = LimitOrderRequest(
                    symbol=ticker,
                    limit_price=round(limit_price, 2),
                    qty=quantity,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.DAY
                )
                order = self.client.submit_order(order_data)
                
                if order.filled_at:
                    fill_price = float(order.filled_avg_price)
                else:
                    # Store for monitoring
                    self.pending_orders[order.id] = {
                        'ticket_id': ticket_id,
                        'ticker': ticker,
                        'expected_price': limit_price,
                        'quantity': quantity,
                        'order_type': 'LIMIT_EXIT',
                        'reason': reason,
                        'submitted_at': datetime.utcnow()
                    }
                    
                    return {
                        'ticket_id': ticket_id,
                        'order_id': order.id,
                        'status': 'PENDING',
                        'ticker': ticker,
                        'quantity': quantity,
                        'limit_price': limit_price,
                        'reason': reason,
                        'message': 'Exit order submitted, waiting for fill'
                    }
            
            # Record execution
            self.slippage.record_execution(
                ticket_id=ticket_id,
                ticker=ticker,
                expected_price=limit_price or fill_price,
                actual_price=fill_price,
                expected_quantity=quantity,
                actual_quantity=quantity,
                order_type=order_type,
                side='SELL'
            )
            
            logger.info(f"Executed {order_type} SELL {quantity} {ticker} @ ${fill_price:.2f} ({reason})")
            
            return {
                'ticket_id': ticket_id,
                'order_id': order.id,
                'status': 'FILLED',
                'ticker': ticker,
                'quantity': quantity,
                'fill_price': fill_price,
                'reason': reason,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Exit failed for {ticker}: {e}")
            return {
                'ticket_id': ticket_id,
                'status': 'FAILED',
                'ticker': ticker,
                'error': str(e)
            }
    
    def check_pending_orders(self) -> Dict[str, Any]:
        """
        Check status of pending orders.
        Updates positions for filled orders.
        """
        if not self.pending_orders:
            return {'checked': 0, 'filled': []}
        
        filled = []
        for order_id, order_info in list(self.pending_orders.items()):
            try:
                order = self.client.get_order_by_id(order_id)
                
                if order.filled_at:
                    # Order filled
                    fill_price = float(order.filled_avg_price)
                    
                    # Record execution
                    self.slippage.record_execution(
                        ticket_id=order_info['ticket_id'],
                        ticker=order_info['ticker'],
                        expected_price=order_info['expected_price'],
                        actual_price=fill_price,
                        expected_quantity=order_info['quantity'],
                        actual_quantity=float(order.filled_qty),
                        order_type=order_info['order_type'],
                        side='BUY' if 'EXIT' not in order_info['order_type'] else 'SELL'
                    )
                    
                    filled.append({
                        'ticket_id': order_info['ticket_id'],
                        'ticker': order_info['ticker'],
                        'fill_price': fill_price,
                        'quantity': float(order.filled_qty),
                        'order_id': order_id
                    })
                    
                    # Remove from pending
                    del self.pending_orders[order_id]
                    
                    logger.info(f"Pending order {order_id} filled at ${fill_price}")
                    
                elif order.canceled_at or order.rejected_at:
                    # Order failed/canceled
                    logger.warning(f"Order {order_id} canceled/rejected")
                    del self.pending_orders[order_id]
                    
            except Exception as e:
                logger.error(f"Error checking order {order_id}: {e}")
        
        return {
            'checked': len(self.pending_orders) + len(filled),
            'filled': filled,
            'still_pending': len(self.pending_orders)
        }
    
    def cancel_pending_order(self, order_id: str) -> bool:
        """Cancel a pending order."""
        try:
            self.client.cancel_order_by_id(order_id)
            if order_id in self.pending_orders:
                del self.pending_orders[order_id]
            logger.info(f"Cancelled order {order_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False
    
    def get_position(self, ticker: str) -> Optional[Dict]:
        """Get current position for a symbol."""
        try:
            position = self.client.get_open_position(ticker)
            return {
                'ticker': ticker,
                'quantity': float(position.qty),
                'avg_entry_price': float(position.avg_entry_price),
                'current_price': float(position.current_price),
                'unrealized_pl': float(position.unrealized_pl),
                'unrealized_plpc': float(position.unrealized_plpc)
            }
        except Exception:
            return None