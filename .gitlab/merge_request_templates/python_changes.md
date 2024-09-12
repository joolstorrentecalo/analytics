Closes

#### List and Describe Code Changes <!-- focus on why the changes are being made-->

* `change & why it was made`

#### Steps Taken to Test

* _action items_

#### Code Quality

* [ ] Passed all Python CI checks?
* [ ] Function signatures contain [type hints](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/#type-hints)?
* [ ] Imports follow PEP8 [rules for ordering](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/#import-order) and there are no extra imports
* [ ] [Docstrings](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/#docstrings) are found in every single function
* [ ] Was a [unit test](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/#unit-testing) added for any complex in-code logic?
  - [ ] Yes
  - [ ] No: <!--explain why-->
* [ ] Are [exceptions and errors](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/#exception-handling) handled correctly?
* [ ] Is there any data manipulation that [should be done in SQL instead](https://about.gitlab.com/handbook/business-technology/data-team/platform/python-guide/#when-not-to-use-python)?

#### Security Merge Request Review

**Detailed Description:**

- Summary of changes made
- Reasons for changes
- Link to related issue/ticket

**Files Changed**:

- List of files changed

**Testing**:

- Summary of testing performed
- Test cases created/modified

**Potential Impact**:

- Describe the impact of the changes on the system/service

**Security Considerations**:

- Highlight any security-related changes
- Data handling/processing changes
- Changes related to [Secure Coding Practices/Guidelines](https://internal.gitlab.com/handbook/enterprise-data/data-platform-security/secure-coding-guidelines/)

**Areas of Focus:**

- [List of areas you want the reviewers and security engineer to focus on]

**Data Engineer Checklists:**

- [ ] Code follows naming conventions and free of syntax errors
- [ ] Code is well-commented
- [ ] Dependencies and third-party libraries are up-to-date and secure
- [ ] Automated tests passed
- [ ] Code follows Secure Coding Standards
- [ ] Relevant documentation is updated
- [ ] Code keeps the code quality standards

**Reviewer Checklist**:

- [ ] Code quality
- [ ] Functionality
- [ ] Feedback
- [ ] Approval

**Security Engineer Checklist**:

- [ ] Thorough security review
- [ ] Feedback/Guidance to fix identified vulnerability
- [ ] Approval

**Notes**:

- Additional notes for reviewers and security engineer

**ETA**:

- Estimated time for review completion

**Notes**:

- Please pay special attention to any changes involving user data or system access
- Consider potential security implications of new features or modifications
- Highlight any security improvements or potential risks introduced by the changes