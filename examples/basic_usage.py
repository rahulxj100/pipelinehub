"""
Basic usage examples for flexible-datapipeline.
"""

from pipelinehub import DataPipeline, normalize_data, square_numbers, add_constant


def main():
    # Example 1: Basic pipeline
    print("=== Basic Pipeline Example ===")
    
    pipeline = DataPipeline()
    pipeline.add_step(lambda x: [i for i in x if i > 0], "filter_positive")
    pipeline.add_step(square_numbers, "square")
    pipeline.add_step(normalize_data, "normalize")
    
    sample_data = [-2, -1, 0, 1, 2, 3, 4, 5]
    result = pipeline.execute(sample_data, verbose=True)
    print(f"Final result: {result}")
    
    # Example 2: Method chaining
    print("\n=== Method Chaining Example ===")
    
    result2 = (DataPipeline()
               .add_step(lambda x: [i for i in x if i % 2 == 0], "filter_even")
               .add_step(add_constant(10), "add_10")
               .add_step(lambda x: sorted(x, reverse=True), "sort_desc")
               .execute([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], verbose=True))
    
    print(f"Final result: {result2}")


if __name__ == "__main__":
    main()