import pytest

from app.adapters.qqbot.adapter import QQBotAdapter
from app.message import File, Image, Video, Voice


@pytest.mark.parametrize("attachment, kind", [
    ({"content_type": "image/png", "url": "https://q/a", "filename": "a.png"}, Image),
    ({"content_type": "video/mp4", "url": "https://q/v", "filename": "v.mp4"}, Video),
    ({"content_type": "voice", "url": "https://q/s", "filename": "s.silk"}, Voice),
    ({"content_type": "audio/amr", "url": "https://q/s2"}, Voice),
    ({"content_type": "file", "url": "https://q/f", "filename": "亡者世界正式版.7z", "size": 1024}, File),
    ({"content_type": "application/zip", "url": "https://q/z", "filename": "a.zip"}, File),
    ({"url": "https://q/n", "filename": "clip.mp4"}, Video),
    ({"url": "https://q/n2", "filename": "notes.unknownext"}, File),
    ({"proxy_url": "https://q/p.png"}, Image),
])
def test_attachments_decode_by_content_type(attachment, kind):
    chain = QQBotAdapter().from_platform_format({"content": "看这个", "attachments": [attachment]})

    assert isinstance(chain.segments[-1], kind)


def test_file_attachment_keeps_name_and_size():
    chain = QQBotAdapter().from_platform_format({"attachments": [
        {"content_type": "file", "url": "https://q/f", "filename": "a.7z", "size": "2048"},
    ]})

    seg = chain.segments[0]
    assert (seg.name, seg.size, seg.media.url) == ("a.7z", 2048, "https://q/f")
