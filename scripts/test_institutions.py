"""Run with: .venv/bin/python -m unittest discover -s scripts -p 'test_*.py'."""
import argparse
import csv
import json
import tempfile
import unittest
from pathlib import Path

from seed_stage import COLUMNS, generate_json_files, load_institution_metadata, pg_type


class InstitutionsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.metadata = self.root / 'institutions.json'

    def write_metadata(self, records):
        self.metadata.write_text(json.dumps(records))
        return load_institution_metadata(self.metadata)

    def test_coordinates_and_geo_are_numeric_including_zero(self):
        result = self.write_metadata([
            {'institution_id': 'I1', 'latitude': '48.15', 'longitude': '17.1'},
            {'id': 'I2', 'geo': {'latitude': 0, 'longitude': 0}},
        ])
        self.assertEqual(result['I1']['latitude'], 48.15)
        self.assertEqual(result['I2']['longitude'], 0)
        self.assertEqual(pg_type('latitude'), 'double precision')
        self.assertEqual(pg_type('longitude'), 'double precision')

    def test_invalid_coordinates_fail_before_import(self):
        for latitude, longitude in [(91, 17), (48, -181), (48, None), (None, 17),
                                    (float('nan'), 17), (48, float('inf')), (True, 17)]:
            with self.subTest(latitude=latitude, longitude=longitude):
                with self.assertRaises(ValueError):
                    self.write_metadata([{'id': 'I1', 'latitude': latitude, 'longitude': longitude}])

    def test_duplicate_metadata_is_rejected(self):
        with self.assertRaises(ValueError):
            self.write_metadata([{'id': 'I1'}, {'id': 'I1'}])

    def test_shared_work_is_counted_once_per_institution(self):
        self.write_metadata([{'id': 'I1', 'latitude': 48.15, 'longitude': 17.1}])
        source = self.root / 'works.json'
        source.write_text(json.dumps({'data': [{
            'id': 'W1', 'title': 'Joint work', 'publication_year': 2024, 'cited_by_count': 7,
            'full_authors_info': 'A1, , Alice, Dept, I1, Institute One, education, SK; '
                                 'A2, , Bob, Dept, I1, Institute One, education, SK; '
                                 'A3, , Carol, Dept, I2, Institute Two, education, CZ',
        }]}))
        args = argparse.Namespace(input_json=source, input_limit=0, institutions_json=self.metadata)
        counts = generate_json_files(args, self.root)
        self.assertEqual(counts['work_institutions'], 3)
        with (self.root / 'institutions.tsv').open() as handle:
            rows = list(csv.DictReader(handle, fieldnames=COLUMNS['institutions'], delimiter='\t'))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['works_count'], '1')
        self.assertEqual(rows[0]['cited_by_count'], '7')
        self.assertEqual(float(rows[0]['latitude']), 48.15)
        self.assertEqual(rows[1]['latitude'], r'\N')
        self.assertEqual(rows[1]['longitude'], r'\N')


if __name__ == '__main__':
    unittest.main()
