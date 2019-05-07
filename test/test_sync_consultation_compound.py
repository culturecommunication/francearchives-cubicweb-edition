# -*- coding: utf-8 -*-
import unittest
from cubicweb import Binary

from cubicweb.devtools.testlib import CubicWebTC
from utils import FrACubicConfigMixIn


class CompoundTests(FrACubicConfigMixIn, CubicWebTC):
    """test ICompound adpaters"""

    def test_news_components(self):
        """ensure each subcomponent of a news has the news as root"""
        with self.admin_access.cnx() as cnx:
            fobj = cnx.create_entity('File',
                                     data=Binary('some-image-data'),
                                     data_name=u'image-name.png',
                                     data_format=u'image/png')
            metadata = cnx.create_entity('Metadata', title=u'meta')
            image = cnx.create_entity('Image',
                                      caption=u'image-caption',
                                      image_file=fobj)
            news = cnx.create_entity('NewsContent',
                                     title=u'news', start_date=u'2016-01-01',
                                     news_image=image,
                                     metadata=metadata)
            cnx.commit()
            self.assertIsNone(news.cw_adapt_to('ICompound'))
            self.assertEqual(image.cw_adapt_to('ICompound').root.eid, news.eid)
            self.assertEqual(fobj.cw_adapt_to('ICompound').root.eid, news.eid)
            self.assertEqual(metadata.cw_adapt_to('ICompound').root.eid, news.eid)

    def test_circular_components(self):
        """ensure each subcomponent of a circular has the circular as root"""
        with self.admin_access.cnx() as cnx:
            fobj1 = cnx.create_entity('File',
                                      data=Binary('some-file-data'),
                                      data_name=u'file1.pdf',
                                      data_format=u'application/pdf')
            fobj2 = cnx.create_entity('File',
                                      data=Binary('some-file-data2'),
                                      data_name=u'file2.pdf',
                                      data_format=u'application/pdf')
            circular = cnx.create_entity('Circular', circ_id=u'c1',
                                         status=u'revoked', title=u'c1',
                                         attachment=fobj1,
                                         additional_attachment=fobj2)
            text = cnx.create_entity('OfficialText', code=u't1',
                                     circular=circular)
            cnx.commit()
            self.assertIsNone(circular.cw_adapt_to('ICompound'))
            self.assertEqual(text.cw_adapt_to('ICompound').root.eid, circular.eid)
            self.assertEqual(fobj1.cw_adapt_to('ICompound').root.eid, circular.eid)
            self.assertEqual(fobj2.cw_adapt_to('ICompound').root.eid, circular.eid)


if __name__ == '__main__':
    unittest.main()
