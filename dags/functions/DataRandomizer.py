import numpy as np
import pandas as pd
import random
from faker import Faker
from typing import List, Dict, Union, Optional
from datetime import datetime, timedelta


class RandomDatasetGenerator:

    def __init__(self, seed: Optional[int] = None, locale: Union[str, List[str]] = 'en_US'):
        self.seed = seed
        if seed is not None:
            np.random.seed(seed)
            random.seed(seed)

        self.faker = Faker(locale)
        if seed is not None:
            Faker.seed(seed)

        self.distributions = {
            "normal": np.random.normal,
            "uniform": np.random.uniform,
            "poisson": np.random.poisson,
            "exponential": np.random.exponential,
            "binomial": np.random.binomial,
            "bernoulli": lambda p, size: np.random.binomial(1, p, size),
            "lognormal": np.random.lognormal,
            "pareto": np.random.pareto,
            "geometric": np.random.geometric,
            "gamma": np.random.gamma,
            "beta": np.random.beta,
            "weibull": np.random.weibull,
            "chisquare": np.random.chisquare,
            "rayleigh": np.random.rayleigh,
            "zipf": np.random.zipf
        }

        # Common data type generators
        self.data_type_generators = {
            # Numeric and basic types
            "integer": self._generate_integers,
            "float": self._generate_floats,
            "boolean": self._generate_booleans,
            "category": self._generate_categories,
            "datetime": self._generate_datetimes,

            # Person related
            "name": self._generate_names,
            "first_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.first_name() for _ in range(n)])),
            "last_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.last_name() for _ in range(n)])),
            "prefix": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.prefix() for _ in range(n)])),
            "suffix": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.suffix() for _ in range(n)])),
            "gender": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.random_element(elements=('M', 'F', 'NB')) for _ in range(n)])),
            "age": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.random_int(**p) if p else self.faker.random_int(min=18, max=90) for _ in range(n)])),
            "birthdate": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.date_of_birth(**p) if p else self.faker.date_of_birth() for _ in range(n)])),
            "ssn": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.ssn() for _ in range(n)])),

            # Internet related
            "email": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.email() for _ in range(n)])),
            "username": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.user_name() for _ in range(n)])),
            "password": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.password(**p) if p else self.faker.password() for _ in range(n)])),
            "domain": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.domain_name() for _ in range(n)])),
            "url": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.url() for _ in range(n)])),
            "ipv4": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.ipv4() for _ in range(n)])),
            "ipv6": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.ipv6() for _ in range(n)])),
            "mac_address": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.mac_address() for _ in range(n)])),
            "user_agent": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.user_agent() for _ in range(n)])),

            # Address related
            "address": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.address() for _ in range(n)])),
            "street_address": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.street_address() for _ in range(n)])),
            "city": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.city() for _ in range(n)])),
            "state": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.state() for _ in range(n)])),
            "state_abbr": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.state_abbr() for _ in range(n)])),
            "zipcode": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.zipcode() for _ in range(n)])),
            "country": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.country() for _ in range(n)])),
            "country_code": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.country_code() for _ in range(n)])),
            "latitude": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([float(self.faker.latitude()) for _ in range(n)])),
            "longitude": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([float(self.faker.longitude()) for _ in range(n)])),
            "coordinates": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([f"{self.faker.latitude()}, {self.faker.longitude()}" for _ in range(n)])),

            # Phone related
            "phone_number": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.phone_number() for _ in range(n)])),
            "msisdn": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.msisdn() for _ in range(n)])),
            "international_phone": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.phone_number() for _ in range(n)])),

            # Company related
            "company": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.company() for _ in range(n)])),
            "company_suffix": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.company_suffix() for _ in range(n)])),
            "job": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.job() for _ in range(n)])),
            "industry": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.job() for _ in range(n)])),

            # Financial
            "credit_card_number": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.credit_card_number(**p) if p else self.faker.credit_card_number() for _ in range(n)])),
            "credit_card_provider": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.credit_card_provider() for _ in range(n)])),
            "credit_card_expire": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.credit_card_expire() for _ in range(n)])),
            "credit_card_security_code": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.credit_card_security_code() for _ in range(n)])),
            "currency_code": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.currency_code() for _ in range(n)])),
            "currency_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.currency_name() for _ in range(n)])),

            # Text content
            "text": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.text(**p) if p else self.faker.text() for _ in range(n)])),
            "paragraph": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.paragraph(**p) if p else self.faker.paragraph() for _ in range(n)])),
            "sentence": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.sentence(**p) if p else self.faker.sentence() for _ in range(n)])),
            "word": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.word() for _ in range(n)])),

            # Color
            "color_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.color_name() for _ in range(n)])),
            "hex_color": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.hex_color() for _ in range(n)])),
            "rgb_color": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.rgb_color() for _ in range(n)])),

            # Various identifiers
            "uuid4": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([str(self.faker.uuid4()) for _ in range(n)])),
            "isbn10": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.isbn10() for _ in range(n)])),
            "isbn13": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.isbn13() for _ in range(n)])),
            "ean8": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.ean8() for _ in range(n)])),
            "ean13": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.ean13() for _ in range(n)])),

            # Dates and times
            "date": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.date(**p) if p else self.faker.date() for _ in range(n)])),
            "time": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.time() for _ in range(n)])),
            "day_of_week": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.day_of_week() for _ in range(n)])),
            "month_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.month_name() for _ in range(n)])),
            "timezone": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.timezone() for _ in range(n)])),

            # Miscellaneous
            "file_path": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.file_path(**p) if p else self.faker.file_path() for _ in range(n)])),
            "file_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.file_name() for _ in range(n)])),
            "mime_type": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.mime_type() for _ in range(n)])),
            "image_url": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.image_url() for _ in range(n)])),
            "user_name": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.user_name() for _ in range(n)])),
            "emoji": lambda n, d, p, c: self._apply_choices_or_generate(n, c, lambda: np.array([self.faker.emoji() for _ in range(n)])),

            # Custom generator for full flexibility
            "custom": self._generate_custom,
        }

    def _apply_choices_or_generate(self, n_rows: int, choices: Optional[List], generator_func):
        """Apply choices if provided, otherwise use the generator function"""
        if choices:
            return np.random.choice(choices, size=n_rows)
        else:
            return generator_func()

    def generate_dataset(
        self,
        n_rows: int,
        columns_config: List[Dict],
        include_index: bool = True,
    ) -> pd.DataFrame:
        """
        Generate a random dataset based on the provided configuration.

        Args:
            n_rows: Number of rows to generate
            columns_config: List of column configurations
            include_index: Whether to include a default index (True) or create a custom one

        Returns:
            Pandas DataFrame with random data

        Example:
            >>> config = [
            ...     {"name": "age", "type": "integer", "params": {"min": 18, "max": 65}},
            ...     {"name": "income", "type": "float", "distribution": "normal", "params": {"loc": 50000, "scale": 15000}},
            ...     {"name": "name", "type": "name", "choices": ["Alice", "Bob", "Charlie"]},
            ...     {"name": "email", "type": "email"},
            ...     {"name": "job", "type": "job"}
            ... ]
            >>> generator = RandomDatasetGenerator(seed=42)
            >>> df = generator.generate_dataset(n_rows=100, columns_config=config)
        """
        data = {}

        for col_config in columns_config:
            col_name = col_config["name"]
            col_type = col_config["type"]
            params = col_config.get("params", {})
            choices = col_config.get("choices", None)

            if "depends_on" in col_config:
                dependency = col_config["depends_on"]
                if dependency["column"] not in data:
                    raise ValueError(
                        f"Column {col_name} depends on {dependency['column']} which hasn't been generated yet")

                dependency_data = data[dependency["column"]]
                data[col_name] = self._generate_dependent_column(
                    n_rows, col_type, dependency_data, dependency["function"], params
                )
                continue

            if col_type in self.data_type_generators:
                distribution = col_config.get("distribution", None)

                # Check if the generator expects choices parameter
                generator = self.data_type_generators[col_type]
                try:
                    # Try to call with choices parameter
                    data[col_name] = generator(
                        n_rows, distribution, params, choices)
                except TypeError:
                    # Fallback for generators that don't support choices
                    if choices:
                        data[col_name] = np.random.choice(choices, size=n_rows)
                    else:
                        data[col_name] = generator(
                            n_rows, distribution, params)
            else:
                raise ValueError(f"Unknown column type: {col_type}")

        df = pd.DataFrame(data)

        # Handle index
        if not include_index or isinstance(include_index, dict):
            if isinstance(include_index, dict):
                index_config = include_index
                index_type = index_config.get("type", "range")

                if index_type == "range":
                    start = index_config.get("start", 1)
                    step = index_config.get("step", 1)
                    df.index = range(start, start + n_rows * step, step)
                elif index_type == "datetime":
                    start_date = index_config.get("start", datetime.now())
                    if isinstance(start_date, str):
                        start_date = datetime.fromisoformat(start_date)
                    freq = index_config.get("freq", "D")
                    df.index = pd.date_range(
                        start=start_date, periods=n_rows, freq=freq)
                elif index_type == "uuid":
                    df.index = [str(self.faker.uuid4()) for _ in range(n_rows)]
                elif index_type == "custom":
                    custom_index = index_config.get("values")
                    if custom_index and len(custom_index) >= n_rows:
                        df.index = custom_index[:n_rows]
                    else:
                        raise ValueError(
                            "Custom index must have at least n_rows values")
            else:
                df.reset_index(drop=True, inplace=True)

        return df

    def _generate_from_distribution(
        self, distribution: str, n_rows: int, params: Dict
    ) -> np.ndarray:
        """Generate values from a specified distribution"""
        if distribution not in self.distributions:
            raise ValueError(f"Unknown distribution: {distribution}")

        try:
            dist_params = {**params, "size": n_rows}
            if distribution == "bernoulli":
                p = params.get("p", 0.5)
                return self.distributions[distribution](p, n_rows)
            else:
                size = dist_params.pop("size")
                return self.distributions[distribution](**dist_params, size=size)
        except TypeError as e:
            supported_params = {
                "normal": ["loc", "scale"],
                "uniform": ["low", "high"],
                "poisson": ["lam"],
                "exponential": ["scale"],
                "binomial": ["n", "p"],
                "bernoulli": ["p"],
                "lognormal": ["mean", "sigma"],
                "pareto": ["a"],
                "geometric": ["p"],
                "gamma": ["shape", "scale"],
                "beta": ["a", "b"],
                "weibull": ["a"],
                "chisquare": ["df"],
                "rayleigh": ["scale"],
                "zipf": ["a"]
            }
            raise TypeError(
                f"Invalid parameters for {distribution} distribution. Supported parameters: {supported_params.get(distribution, 'unknown')}") from e

    def _generate_integers(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """Generate integer values"""
        if choices:
            return np.random.choice(choices, size=n_rows)

        if distribution:
            # Generate using specified distribution then convert to integers
            values = self._generate_from_distribution(
                distribution, n_rows, params)
            return values.astype(int)
        else:
            # Handle both 'min'/'max' and 'low'/'high' parameters for compatibility
            min_val = params.get("min", params.get("low", 0))
            max_val = params.get("max", params.get("high", 100))
            return np.random.randint(min_val, max_val + 1, size=n_rows)

    def _generate_floats(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """Generate float values"""
        if choices:
            return np.random.choice(choices, size=n_rows)

        if distribution:
            return self._generate_from_distribution(distribution, n_rows, params)
        else:
            min_val = params.get("min", params.get("low", 0.0))
            max_val = params.get("max", params.get("high", 1.0))
            return np.random.uniform(min_val, max_val, size=n_rows)

    def _generate_booleans(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """Generate boolean values"""
        if choices:
            return np.random.choice(choices, size=n_rows)

        p_true = params.get("p_true", 0.5)
        return np.random.random(size=n_rows) < p_true

    def _generate_categories(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """Generate categorical values"""
        if choices:
            weights = params.get("weights", None)
            return np.random.choice(choices, size=n_rows, p=weights)

        categories = params.get("categories", ["A", "B", "C"])
        weights = params.get("weights", None)

        if weights and len(weights) != len(categories):
            raise ValueError(
                "Number of weights must match number of categories")
        return np.random.choice(categories, size=n_rows, p=weights)

    def _generate_datetimes(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """Generate datetime values"""
        if choices:
            return np.random.choice(choices, size=n_rows)

        start_date = params.get("start", "2020-01-01T00:00:00")
        end_date = params.get("end", "2025-01-01T00:00:00")

        # Convert string dates to datetime objects if needed
        if isinstance(start_date, str):
            start_date = datetime.fromisoformat(start_date)
        if isinstance(end_date, str):
            end_date = datetime.fromisoformat(end_date)

        if distribution:
            # Use Faker's date_time_between for more control
            return np.array([
                self.faker.date_time_between(
                    start_date=start_date, end_date=end_date)
                for _ in range(n_rows)
            ])
        else:
            # Calculate time range in seconds
            time_range = (end_date - start_date).total_seconds()

            # Generate random timestamps within the range
            random_seconds = np.random.uniform(0, time_range, size=n_rows)
            return np.array([start_date + timedelta(seconds=sec) for sec in random_seconds])

    def _generate_names(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """Generate names using Faker"""
        if choices:
            return np.random.choice(choices, size=n_rows)

        name_type = params.get("name_type", "full")  # full, first, last

        if name_type == "first":
            return np.array([self.faker.first_name() for _ in range(n_rows)])
        elif name_type == "last":
            return np.array([self.faker.last_name() for _ in range(n_rows)])
        else:  # full
            return np.array([self.faker.name() for _ in range(n_rows)])

    def _generate_dependent_column(
        self, n_rows: int, col_type: str, dependency_data: np.ndarray,
        function_type: str, params: Dict
    ) -> np.ndarray:
        """
        Generate a column that depends on another column.

        Args:
            n_rows: Number of rows
            col_type: Target column type
            dependency_data: Data from the column this depends on
            function_type: Type of function to apply ('transform', 'map', 'custom')
            params: Additional parameters

        Returns:
            Generated dependent column data
        """
        if function_type == "transform":
            # Apply a transformation function
            transform_type = params.get("transform_type", "add")
            value = params.get("value", 1)

            if transform_type == "add":
                result = dependency_data + value
            elif transform_type == "subtract":
                result = dependency_data - value
            elif transform_type == "multiply":
                result = dependency_data * value
            elif transform_type == "divide":
                result = dependency_data / value
            elif transform_type == "power":
                result = dependency_data ** value
            elif transform_type == "log":
                result = np.log(dependency_data)
            elif transform_type == "exp":
                result = np.exp(dependency_data)
            elif transform_type == "abs":
                result = np.abs(dependency_data)
            elif transform_type == "round":
                decimals = params.get("decimals", 0)
                result = np.round(dependency_data, decimals)
            else:
                raise ValueError(f"Unknown transform type: {transform_type}")

            return result

        elif function_type == "map":
            mapping = params.get("mapping", {})
            default = params.get("default", None)

            # Convert mapping keys to appropriate type if needed
            if dependency_data.dtype == np.dtype('bool'):
                mapping = {bool(k): v for k, v in mapping.items()}
            elif np.issubdtype(dependency_data.dtype, np.integer):
                mapping = {int(k): v for k, v in mapping.items()}
            elif np.issubdtype(dependency_data.dtype, np.floating):
                mapping = {float(k): v for k, v in mapping.items()}

            # Apply mapping
            result = np.array([mapping.get(x, default)
                              for x in dependency_data])
            return result

        elif function_type == "custom":
            custom_func = params.get("function")
            if not custom_func:
                raise ValueError("Custom function not provided")

            # Convert string function to lambda
            if isinstance(custom_func, str):
                if custom_func.startswith("x:"):
                    try:
                        func = eval(f"lambda {custom_func}")
                        return np.array([func(x) for x in dependency_data])
                    except Exception as e:
                        raise ValueError(
                            f"Error evaluating custom function: {e}")

            return dependency_data

        else:
            raise ValueError(f"Unknown function type: {function_type}")

    def _generate_custom(
        self, n_rows: int, distribution: Optional[str], params: Dict, choices: Optional[List] = None
    ) -> np.ndarray:
        """
        Generate custom values using a provided function.

        The function can be:
        1. A Faker provider method name (e.g., "license_plate")
        2. A lambda or function in string form (e.g., "lambda i: f'CUSTOM-{i}'")
        3. A dict with 'choices' to select from
        """
        if choices:
            weights = params.get("weights")
            return np.random.choice(choices, size=n_rows, p=weights)

        faker_method = params.get("faker_method")
        if faker_method:
            # Use a specific Faker provider method
            try:
                provider_method = getattr(self.faker, faker_method)
                return np.array([provider_method() for _ in range(n_rows)])
            except AttributeError:
                raise ValueError(f"Unknown Faker method: {faker_method}")

        param_choices = params.get("choices")
        if param_choices:
            # Select from provided choices in params
            weights = params.get("weights")
            return np.random.choice(param_choices, size=n_rows, p=weights)

        # Default to generated IDs
        prefix = params.get("prefix", "ID")
        delimiter = params.get("delimiter", "_")
        start = params.get("start", 1)
        return np.array([f"{prefix}{delimiter}{start + i}" for i in range(n_rows)])
