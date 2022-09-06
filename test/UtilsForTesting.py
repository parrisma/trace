class UtilsForTesting:
    MARGIN_OF_ERROR: float = 1e-06

    @classmethod
    def test_case(cls,
                  func):
        def annotated_test_case(*args, **kwargs):
            print(f'- - - - - - R U N  {func.__name__}  - - - - - -')
            func(*args, **kwargs)

        return annotated_test_case
