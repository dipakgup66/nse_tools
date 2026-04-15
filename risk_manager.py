import yaml
import os
import math
import datetime

CONFIG_PATH = r"c:\Users\HP\nse_tools\config.yaml"

class RiskManager:
    def __init__(self, config_path=CONFIG_PATH):
        self.config_path = config_path
        self.config = self._load_config()

    def _load_config(self):
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found at {self.config_path}")
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def validate_entry(self, signal, underlying_ltp, prev_close):
        """
        Validates if a new trade entry is allowed based on calendar and gaps.
        Returns (is_valid, reason_if_invalid)
        """
        # 1. Calendar Event Check
        today_str = datetime.date.today().strftime('%Y-%m-%d')
        skip_days = self.config.get('event_calendar', {}).get('skip_trading_days', [])
        if today_str in skip_days:
            return False, f"Event Calendar blocked entry: {today_str} is a restricted day."

        # 2. Overnight Gap Check
        max_gap_pct = self.config.get('event_calendar', {}).get('max_overnight_gap_pct', 1.0)
        gap_pct = abs((underlying_ltp - prev_close) / prev_close) * 100
        if gap_pct > max_gap_pct:
            return False, f"Market Gap > {max_gap_pct}% ({gap_pct:.2f}%). Entry paused to let volatility settle."

        # 3. Confidence & Expectancy checks are handled by the Regime Generator/Router now.
        return True, "Valid Entry"

    def calculate_position_size(self, strategy_type, underlying_price, target_loss_per_lot):
        """
        Calculates how many lots to trade to cap risk at max_loss_pct_capital.
        For undefined risk strategies, we require target_loss_per_lot to be predefined (e.g., synthetic 3-sigma expected max loss).
        """
        capital = self.config.get('risk_limits', {}).get('total_capital', 1000000)
        max_loss_pct = self.config.get('risk_limits', {}).get('max_loss_pct_capital', 1.5)
        
        max_absolute_loss = capital * (max_loss_pct / 100.0)
        
        if target_loss_per_lot <= 0:
            return 0 # Invalid
            
        allowed_lots = math.floor(max_absolute_loss / target_loss_per_lot)
        return allowed_lots

    def check_kill_switches(self, entry_vix, current_vix, entry_price, current_price, current_mtm):
        """
        Monitors live market data against Kill Switches.
        Returns (trigger_fired, reason)
        """
        kill = self.config.get('kill_switches', {})
        max_vix_spike = kill.get('vix_spike_pct', 20.0)
        max_price_move = kill.get('underlying_move_pct', 1.5)
        
        # 1. VIX Spike Check
        if entry_vix and current_vix:
            vix_spike = ((current_vix - entry_vix) / entry_vix) * 100
            if vix_spike > max_vix_spike:
                return True, f"VIX Spike Kill Switch: VIX rose {vix_spike:.1f}% from entry (Threshold: {max_vix_spike}%)"

        # 2. Underlying Move Check
        if entry_price and current_price:
            price_move = (abs(current_price - entry_price) / entry_price) * 100
            if price_move > max_price_move:
                return True, f"Price Move Kill Switch: Underlying moved {price_move:.2f}% from entry (Threshold: {max_price_move}%)"

        # 3. Global MTM Loss Check
        capital = self.config.get('risk_limits', {}).get('total_capital', 1000000)
        max_mtm_pct = self.config.get('risk_limits', {}).get('max_mtm_loss_pct', 2.0)
        max_mtm_loss_abs = capital * (max_mtm_pct / 100.0)
        
        if current_mtm < -max_mtm_loss_abs:
            return True, f"Global MTM Kill Switch: Loss exceeded limit {max_mtm_loss_abs}."

        return False, "Safe"
