from webcompat_kb.etl.web_bugs import BodyData, ConfigurationValue, WebBugsRepo


def test_parse_issue_body_webcompat():
    data = """<!-- @browser: Firefox 152.0 -->
<!-- @ua_header: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0 -->
<!-- @reported_with: desktop-reporter -->
<!-- @public_url: https://github.com/webcompat/web-bugs/issues/226261 -->

**URL**: https://www.rti.org.tw/

**Browser / Version**: Firefox 152.0
**Operating System**: Windows 10
**Tested Another Browser**: Yes Other

**Problem type**: Site is not usable
**Description**: Page not loading correctly
**Steps to Reproduce**:
This web repeatedly not load via proxy mode configured for cloudflare one client desktop version.

<details>
      <summary>View the screenshot</summary>
      <img alt="Screenshot" src="https://webcompat.com/uploads/2026/6/22f6c7a3-dd29-4607-b0c9-0f323b6dcab8.jpeg">
      </details>

<details>
<summary>Browser Configuration</summary>
<ul>
  <li>blockList: basic</li><li>channel: release</li><li>defaultUserAgent: Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0</li><li>gfx.webrender.software: false</li><li>hasTouchScreen: false</li><li>frameworks: {'fastclick': False, 'marfeel': False, 'mobify': False}</li><li>mixed active content blocked: false</li><li>mixed passive content blocked: false</li><li>tracking content blocked: false</li><li>btp has purged site: false</li>
</ul>
</details>

[View console log messages](https://webcompat.com/console_logs/2026/6/07a580c5-ebcc-44e2-b518-00f9fbbcbc00)

_From [webcompat.com](https://webcompat.com/) with ❤️_
"""
    assert WebBugsRepo(None, None).parse_issue_body(1, data) == BodyData(
        ua_header="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
        source="desktop-reporter",
        bug_url="https://www.rti.org.tw/",
        browser="Firefox 152.0",
        os="Windows 10",
        problem_type="Site is not usable",
        description="Page not loading correctly",
        steps_to_reproduce="This web repeatedly not load via proxy mode configured for cloudflare one client desktop version.",
        screenshot="https://webcompat.com/uploads/2026/6/22f6c7a3-dd29-4607-b0c9-0f323b6dcab8.jpeg",
        configuration=[
            ConfigurationValue("blockList", "basic"),
            ConfigurationValue("channel", "release"),
            ConfigurationValue(
                "defaultUserAgent",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
            ),
            ConfigurationValue("gfx.webrender.software", "false"),
            ConfigurationValue("hasTouchScreen", "false"),
            ConfigurationValue(
                "frameworks", "{'fastclick': False, 'marfeel': False, 'mobify': False}"
            ),
            ConfigurationValue("mixed active content blocked", "false"),
            ConfigurationValue("mixed passive content blocked", "false"),
            ConfigurationValue("tracking content blocked", "false"),
            ConfigurationValue("btp has purged site", "false"),
        ],
    )


def test_parse_issue_body_web_bugs():
    data = """**URL**:
https://myaccount.google.com/signinoptions/two-step-verification

**Browser/Version**:
Firefox 57.0a1 build id 20170914220209
`security.webauth.u2f` set to `true`
`security.webauth.webauthn_enable_usbtoken` set to `true`
`security.webauth.webauthn_enable_softtoken` set to `false`


**Operating System**: Linux

**What seems to be the trouble?(Required)**
- [ ] Desktop site instead of mobile site
- [ ] Mobile site is not usable
- [ ] Video doesn't play
- [ ] Layout is messed up
- [ ] Text is not visible
- [X] Something else (Add details below)

**Steps to Reproduce**

1. Navigate to: https://myaccount.google.com/signinoptions/two-step-verification
2. Attempt to add a U2F device ("Security Key")

*__Expected Behavior:__*
Google let's me add a U2F device from Firefox

*__Actual Behavior:__*
Google informs me that only Chrome supports U2F and to change web browser


**Screenshot**

![screenshot](https://pbs.twimg.com/media/DJxw9baVYAAzdXi.jpg "Google error message")"""
    assert WebBugsRepo(None, None).parse_issue_body(1, data) == BodyData(
        ua_header=None,
        source="web-bugs",
        bug_url="https://myaccount.google.com/signinoptions/two-step-verification",
        browser="Firefox 57.0a1 build id 20170914220209",
        os="Linux",
        problem_type="Something else (Add details below)",
        description="""Expected Behavior:
Google let's me add a U2F device from Firefox

Actual Behavior:
Google informs me that only Chrome supports U2F and to change web browser

""",
        steps_to_reproduce="""1. Navigate to: https://myaccount.google.com/signinoptions/two-step-verification
2. Attempt to add a U2F device ("Security Key")""",
        screenshot="https://pbs.twimg.com/media/DJxw9baVYAAzdXi.jpg",
        configuration=[],
    )
