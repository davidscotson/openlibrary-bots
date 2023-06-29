"""
isbn in indentifiers section to isbn 13
NOTE: This script ideally works on an Open Library Dump that only contains editions with an identfiers.isbn no isbn_13
"""
import gzip
import json
import re

import isbnlib
import olclient


class ConvertISBNidentifierto13Job(olclient.AbstractBotJob):
    def run(self) -> None:
        """Looks for any ISBN in identifiers.isbn to convert to 13"""
        self.write_changes_declaration()
        header = {"type": 0, "key": 1, "revision": 2, "last_modified": 3, "JSON": 4}
        comment = "extract ISBN 13 from identifiers field"
        with gzip.open(self.args.file, "rb") as fin:
            for row_num, row in enumerate(fin):
                row = row.decode().split("\t")
                _json = json.loads(row[header["JSON"]])
                if _json["type"]["key"] != "/type/edition":
                    continue

                if hasattr(_json, "isbn_13"):
                    # we only update editions with no existing isbn 13s (for now at least)
                    continue

                isbns = None
                identifiers = _json.get("identifiers", None)
                if not identifiers:
                    continue
                isbns = identifiers.get("isbn", None)
                if not isbns:
                    continue

                isbns_13 = []
                isbn_13 = None
                for isbn in isbns:
                    if isbnlib.is_isbn10(isbnlib.canonical(isbn)):
                        isbn_13 = isbnlib.to_isbn13(isbnlib.canonical(isbn))
                    if isbnlib.is_isbn13(isbnlib.canonical(isbn)):
                        isbn_13 = isbnlib.canonical(isbn)
                    if isbn_13:
                        isbns_13.append(isbn_13)

                if not isbns_13:
                    continue

                olid = _json["key"].split("/")[-1]
                edition = self.ol.Edition.get(olid)
                if edition.type["key"] != "/type/edition":
                    continue

                if hasattr(edition, "isbn_13"):
                    # don't update editions that already have an isbn 13
                    continue


                setattr(edition, "isbn_13", isbns_13)
                self.logger.info("\t".join([olid, str(isbns), str(isbns_13)]))
                self.save(lambda: edition.save(comment=comment))


if __name__ == "__main__":
    job = ConvertISBNidentifierto13Job()

    try:
        job.run()
    except Exception as e:
        job.logger.exception(e)
        raise e
