"""
Display management for console output formatting and colors.

This module provides a centralized way to handle console output with consistent
formatting, colors, and icons for different message types.
"""

from colorama import Fore, Style, init as colorama_init


class DisplayManager:
    """Handles console output formatting and colors."""
    
    def __init__(self):
        """Initialize colorama for cross-platform color support."""
        colorama_init(autoreset=True)
    
    @staticmethod
    def print_error(content: str) -> None:
        """Print content in red color for errors."""
        print(f"{Fore.RED}❌ {content}{Style.RESET_ALL}")
    
    @staticmethod
    def print_success(content: str) -> None:
        """Print content in green color for success messages."""
        print(f"{Fore.GREEN}✅ {content}{Style.RESET_ALL}")
    
    @staticmethod
    def print_info(content: str) -> None:
        """Print content in yellow color for informational messages."""
        print(f"{Fore.YELLOW}ℹ️  {content}{Style.RESET_ALL}")
    
    @staticmethod
    def print_warning(content: str) -> None:
        """Print content in magenta color for warnings."""
        print(f"{Fore.MAGENTA}⚠️  {content}{Style.RESET_ALL}")
