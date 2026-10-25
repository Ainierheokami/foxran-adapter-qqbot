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


def test_attachment_placeholder_in_content_becomes_the_attachment():
    """QQ puts <attachmentType=...> in the text where the picture was; it is not text."""
    chain = QQBotAdapter().from_platform_format({
        "content": '看这个<attachmentType="image/jpeg",attachmentIndex=0,description="eyJ0ZXh0IjoiW+WbvueJh10ifQ==">好笑',
        "attachments": [{"content_type": "image/jpeg", "url": "https://q/a", "filename": "A.jpg"}],
    })

    assert [type(seg).__name__ for seg in chain.segments] == ["Text", "Image", "Text"]
    assert [seg.text for seg in chain.segments if hasattr(seg, "text")] == ["看这个", "好笑"]
    assert chain.segments[1].media.url == "https://q/a"


def test_placeholder_without_matching_attachment_is_dropped_and_extras_appended():
    chain = QQBotAdapter().from_platform_format({
        "content": '<attachmentType="image/jpeg",attachmentIndex=3,description="x">',
        "attachments": [{"content_type": "image/png", "url": "https://q/b"}],
    })

    assert [type(seg).__name__ for seg in chain.segments] == ["Image"]
    assert chain.segments[0].media.url == "https://q/b"
