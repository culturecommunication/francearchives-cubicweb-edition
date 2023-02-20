# -*- coding: utf-8 -*-
import unittest
from cubicweb import Binary

from cubicweb.devtools.testlib import CubicWebTC
from utils import FrACubicConfigMixIn

from cubicweb_francearchives.testutils import S3BfssStorageTestMixin


class CompoundTests(S3BfssStorageTestMixin, FrACubicConfigMixIn, CubicWebTC):
    """test ICompound adpaters"""

    def test_news_components(self):
        """ensure each subcomponent of a news has the news as root"""
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity(
                "File",
                data=Binary(b"some-image-data"),
                data_name="image-name.png",
                data_format="image/png",
            )
            metadata = cnx.create_entity("Metadata", title="meta")
            image = cnx.create_entity("Image", caption="image-caption", image_file=fobj)
            news = cnx.create_entity(
                "NewsContent",
                title="news",
                start_date="2016-01-01",
                news_image=image,
                metadata=metadata,
            )
            cnx.commit()
            self.assertIsNone(news.cw_adapt_to("ICompound"))
            self.assertEqual(image.cw_adapt_to("ICompound").roots[0].eid, news.eid)
            self.assertEqual(fobj.cw_adapt_to("ICompound").roots[0].eid, news.eid)
            self.assertEqual(metadata.cw_adapt_to("ICompound").roots[0].eid, news.eid)

    def test_circular_components(self):
        """ensure each subcomponent of a circular has the circular as root"""
        with self.admin_access.cnx() as cnx:
            fobj1 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data"),
                data_name="file1.pdf",
                data_format="application/pdf",
            )
            fobj2 = cnx.create_entity(
                "File",
                data=Binary(b"some-file-data2"),
                data_name="file2.pdf",
                data_format="application/pdf",
            )
            circular = cnx.create_entity(
                "Circular",
                circ_id="c1",
                status="revoked",
                title="c1",
                attachment=fobj1,
                additional_attachment=fobj2,
            )
            text = cnx.create_entity("OfficialText", code="t1", circular=circular)
            cnx.commit()
            self.assertIsNone(circular.cw_adapt_to("ICompound"))
            self.assertEqual(text.cw_adapt_to("ICompound").roots[0].eid, circular.eid)
            self.assertEqual(fobj1.cw_adapt_to("ICompound").roots[0].eid, circular.eid)
            self.assertEqual(fobj2.cw_adapt_to("ICompound").roots[0].eid, circular.eid)


if __name__ == "__main__":
    unittest.main()
