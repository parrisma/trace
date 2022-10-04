# ----------------------------------------------------------------------------
# MIT License
#
# Copyright (c) 2022 parris3142
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
# ----------------------------------------------------------------------------
import os
import re
from elastic.ESUtil import ESUtil


class UtilsForTesting:
    MARGIN_OF_ERROR: float = 1e-06

    @classmethod
    def test_case(cls,
                  func):
        def annotated_test_case(*args, **kwargs):
            print(f'- - - - - - R U N  {func.__name__}  - - - - - -')
            func(*args, **kwargs)

        return annotated_test_case

    @staticmethod
    def _delete_all_test_indexes(es_connection):
        # Find any residual indices from failed test clean-ups
        try:
            for candidate in es_connection.cat.indices().body.split():
                if re.match(r'(trace_index_.*|index_handler_.*|index-.*)', candidate):
                    if ESUtil.index_exists(es=es_connection,
                                           idx_name=candidate):
                        ESUtil.delete_index(es=es_connection,
                                            idx_name=candidate)
                        print(f'Cleaned up (deleted) test index {candidate}')

        except Exception as e:
            print(f'Failed to find list of test indices with error {str(e)}')
        return

    @staticmethod
    def _clean_up_test_files():
        dirs_to_clean = [[".", r'.*\.log']]
        for dir_to_clean, pattern in dirs_to_clean:
            files_to_delete = [os.path.join(dir_to_clean, f) for f in os.listdir(dir_to_clean) if re.match(pattern, f)]
            for file_to_delete in files_to_delete:
                print(f'- - - - - - deleting test file {files_to_delete}')
                try:
                    os.remove(file_to_delete)
                except Exception as e:
                    print(f'- - - - - - Warning - failed to delete {files_to_delete} with error {str(e)}')
        return
