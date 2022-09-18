from pathlib import Path


class ElasticResources:
    @classmethod
    def trace_index_definition_file(cls,
                                    resource_root: str) -> str:
        fn = f'{resource_root}\\elastic-log-index.json'
        if not Path(fn).exists():
            raise ValueError(f'Index JSON definition cannot be found at {fn}')
        return fn

    @classmethod
    def trace_index_definition_as_json(cls) -> str:
        return """
               {
                 "properties": {
                   "session_uuid": {
                     "type": "text"
                   },
                   "level": {
                     "type": "text"
                   },
                   "timestamp": {
                     "type": "date",
                     "format": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ"
                   },
                   "message": {
                     "type": "text"
                   }
                 }
               }
        """
