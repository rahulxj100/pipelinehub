"""
Core DataPipeline class for creating flexible data processing workflows.
"""

from typing import Any, Callable, List, Optional, Union


class DataPipeline:
    """
    A flexible data pipeline that allows adding custom processing steps.
    Each step is a function that transforms the data.
    """
    
    def __init__(self, data: Any = None):
        """
        Initialize a new DataPipeline.
        
        Args:
            data: Optional initial data for the pipeline
        """
        self.data = data
        self.steps: List[Callable] = []
        self.step_names: List[str] = []
    
    def add_step(self, func: Callable, name: Optional[str] = None) -> "DataPipeline":
        """
        Add a custom processing step to the pipeline.
        
        Args:
            func: A function that takes data as input and returns transformed data
            name: Optional name for the step (for debugging/logging)
        
        Returns:
            self (for method chaining)
            
        Raises:
            ValueError: If func is not callable
        """
        if not callable(func):
            raise ValueError("Step must be a callable function")
        
        self.steps.append(func)
        step_name = name or getattr(func, '__name__', f"step_{len(self.steps)}")
        self.step_names.append(step_name)
        
        return self
    
    def set_data(self, data: Any) -> "DataPipeline":
        """
        Set the initial data for the pipeline.
        
        Args:
            data: The data to process
            
        Returns:
            self (for method chaining)
        """
        self.data = data
        return self
    
    def execute(self, data: Any = None, verbose: bool = False) -> Any:
        """
        Execute all steps in the pipeline.
        
        Args:
            data: Optional data to process (overrides instance data)
            verbose: Print step-by-step execution info
            
        Returns:
            Transformed data after all steps
            
        Raises:
            ValueError: If no data is provided
            RuntimeError: If any step fails during execution
        """
        current_data = data if data is not None else self.data
        
        if current_data is None:
            raise ValueError("No data provided. Use set_data() or pass data to execute()")
        
        if verbose:
            print(f"Starting pipeline with {len(self.steps)} steps")
            data_info = self._get_data_info(current_data)
            print(f"Initial data: {data_info}")
        
        for i, (step, step_name) in enumerate(zip(self.steps, self.step_names)):
            try:
                if verbose:
                    print(f"\nStep {i+1}: {step_name}")
                
                current_data = step(current_data)
                
                if verbose:
                    data_info = self._get_data_info(current_data)
                    print(f"  Output: {data_info}")
                    
            except Exception as e:
                raise RuntimeError(f"Error in step {i+1} ({step_name}): {str(e)}")
        
        return current_data
    
    def clear_steps(self) -> "DataPipeline":
        """
        Remove all steps from the pipeline.
        
        Returns:
            self (for method chaining)
        """
        self.steps.clear()
        self.step_names.clear()
        return self
    
    def remove_step(self, index: int) -> "DataPipeline":
        """
        Remove a step by index.
        
        Args:
            index: Index of the step to remove
            
        Returns:
            self (for method chaining)
        """
        if 0 <= index < len(self.steps):
            self.steps.pop(index)
            self.step_names.pop(index)
        return self
    
    def get_steps(self) -> List[str]:
        """
        Get list of step names.
        
        Returns:
            List of step names
        """
        return self.step_names.copy()
    
    def _get_data_info(self, data: Any) -> str:
        """Get descriptive info about data for verbose output."""
        data_type = type(data).__name__
        if hasattr(data, '__len__'):
            return f"{data_type} with {len(data)} elements"
        return f"{data_type}"
    
    def __len__(self) -> int:
        """Return number of steps in the pipeline."""
        return len(self.steps)
    
    def __repr__(self) -> str:
        """String representation of the pipeline."""
        steps_str = ', '.join(self.step_names) if self.step_names else "no steps"
        return f"DataPipeline({len(self.steps)} steps: {steps_str})"